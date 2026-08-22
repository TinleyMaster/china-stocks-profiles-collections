"""
Phase D: 投研笔记本（research_notebook）构建

每股一个 notebook，自动汇总：
  1. 基础画像（行情、估值、财务、资金面、股东）
  2. 资料完整性清单（21 项 A 股投研资料类别）
  3. 文档 / 事件统计

完整性清单定义（A 股定制，对齐 crypto 项目的 21 类思路）：
  基础资料类（必备）：
    - annual_report       年报（近 3 年）
    - semi_annual_report  半年报（近 3 年）
    - quarterly_report    季报（近 8 期）
    - prospectus          招股说明书
    - finance_snapshot    财务指标
    - stock_basic         行情估值
  资金面类：
    - capital_snapshot    资金面（北向/两融）
    - shareholder         股东画像
  事件类：
    - dividend            分红送转
    - unlock              限售解禁
    - buyback             回购
    - profit_alert        业绩预告
    - share_change        增减持
  研究类：
    - research_deep       深度报告
    - research_comment    点评报告
    - survey              调研纪要
  其他：
    - announcement_other  其他公告
    - corporate_event     公司事件汇总
    - industry_report     行业研报
    - government_grant    政府补助/政策
    - related_party       关联交易

每项状态：
  done    — 资料齐全（达到阈值）
  partial — 有部分资料
  missing — 完全没有
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from ..src.phase_a_stock_pool import get_stock_codes


# 完整性清单定义
COMPLETENESS_ITEMS: dict[str, dict] = {
    # 基础资料
    "annual_report": {"label": "年报", "category": "基础", "threshold_docs": 3, "doc_types": ["annual_report"]},
    "semi_annual_report": {"label": "半年报", "category": "基础", "threshold_docs": 3, "doc_types": ["semi_annual_report"]},
    "quarterly_report": {"label": "季报", "category": "基础", "threshold_docs": 8, "doc_types": ["quarterly_report"]},
    "prospectus": {"label": "招股说明书", "category": "基础", "threshold_docs": 1, "doc_types": ["prospectus"]},
    "finance_snapshot": {"label": "财务指标", "category": "基础", "threshold_docs": 1, "biz_table": "finance_snapshot"},
    "stock_basic": {"label": "行情估值", "category": "基础", "threshold_docs": 1, "biz_table": "stock_basic"},
    # 资金面
    "capital_snapshot": {"label": "资金面", "category": "资金面", "threshold_docs": 1, "biz_table": "capital_snapshot"},
    "shareholder": {"label": "股东画像", "category": "资金面", "threshold_docs": 1, "biz_table": "shareholder_snapshot"},
    # 事件
    "dividend": {"label": "分红送转", "category": "事件", "threshold_docs": 1, "event_type": "dividend"},
    "unlock": {"label": "限售解禁", "category": "事件", "threshold_docs": 1, "event_type": "unlock"},
    "buyback": {"label": "回购", "category": "事件", "threshold_docs": 1, "event_type": "buyback"},
    "profit_alert": {"label": "业绩预告", "category": "事件", "threshold_docs": 1, "event_type": "profit_alert"},
    "share_change": {"label": "增减持", "category": "事件", "threshold_docs": 1, "event_type": "share_change"},
    # 研究
    "research_deep": {"label": "深度报告", "category": "研究", "threshold_docs": 3, "content_topic": "research_deep"},
    "research_comment": {"label": "点评报告", "category": "研究", "threshold_docs": 5, "content_topic": "research_comment"},
    "survey": {"label": "调研纪要", "category": "研究", "threshold_docs": 3, "doc_types": ["survey"]},
    # 其他
    "announcement_other": {"label": "其他公告", "category": "其他", "threshold_docs": 10, "doc_type_generic": True},
    "corporate_event": {"label": "公司事件汇总", "category": "其他", "threshold_docs": 5, "event_type_any": True},
    "industry_report": {"label": "行业研报", "category": "其他", "threshold_docs": 3, "content_topic": "industry_research"},
    "st_change": {"label": "ST/摘帽", "category": "其他", "threshold_docs": 0, "event_type": "st_change"},
}


@dataclass
class NotebookCompleteness:
    items: dict = field(default_factory=dict)  # {key: {status, count, label, category}}

    def to_json(self) -> str:
        return json.dumps(self.items, ensure_ascii=False)

    def overall_score(self) -> float:
        """计算整体完整度得分（0~100）。"""
        if not self.items:
            return 0.0
        weights = {"基础": 2.0, "资金面": 1.5, "事件": 1.0, "研究": 1.5, "其他": 0.5}
        total_weight = 0.0
        score = 0.0
        for key, item in self.items.items():
            cat = item.get("category", "其他")
            w = weights.get(cat, 1.0)
            total_weight += w
            if item["status"] == "done":
                score += w * 1.0
            elif item["status"] == "partial":
                score += w * 0.5
        return round(score / total_weight * 100, 1) if total_weight > 0 else 0.0


def _calc_item_status(count: int, threshold: int) -> tuple[str, int]:
    """根据数量和阈值判断状态。"""
    if count >= threshold:
        return "done", count
    if count > 0:
        return "partial", count
    return "missing", count


def build_notebook_for_stock(stock_code: str) -> dict:
    """
    为单只股票构建/刷新投研笔记本。
    返回 notebook 摘要信息。
    """
    with get_session() as sess:
        # 1. 股票基础信息
        stock = sess.execute(text("""
            SELECT stock_code, stock_name, market, primary_industry_l1, primary_industry_l2
            FROM core.stock WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()
        if not stock:
            raise ValueError(f"股票不存在: {stock_code}")

        completeness = {}

        # 2. 文档类（按 doc_type 或 content_topics 统计）
        for key, cfg in COMPLETENESS_ITEMS.items():
            count = 0
            threshold = cfg.get("threshold_docs", 1)

            if "doc_types" in cfg:
                # 按 doc_type 统计
                row = sess.execute(text("""
                    SELECT COUNT(*) FROM biz.doc_source_entry
                    WHERE stock_code = :code AND doc_type = ANY(:dtypes)
                """), {"code": stock_code, "dtypes": cfg["doc_types"]}).fetchone()
                count = row[0] or 0

            elif "content_topic" in cfg:
                # 按 content_topics 数组包含匹配
                row = sess.execute(text("""
                    SELECT COUNT(*) FROM biz.doc_source_entry
                    WHERE stock_code = :code AND :topic = ANY(content_topics)
                """), {"code": stock_code, "topic": cfg["content_topic"]}).fetchone()
                count = row[0] or 0

            elif "biz_table" in cfg:
                # 业务表有记录就算
                tbl = cfg["biz_table"]
                if tbl == "shareholder_snapshot":
                    row = sess.execute(text(f"""
                        SELECT COUNT(*) FROM biz.{tbl} WHERE stock_code = :code
                    """), {"code": stock_code}).fetchone()
                else:
                    row = sess.execute(text(f"""
                        SELECT 1 FROM biz.{tbl} WHERE stock_code = :code LIMIT 1
                    """), {"code": stock_code}).fetchone()
                count = 1 if row and row[0] else 0

            elif "event_type" in cfg:
                # 按事件类型统计
                row = sess.execute(text("""
                    SELECT COUNT(*) FROM biz.corporate_event
                    WHERE stock_code = :code AND event_type = :etype
                """), {"code": stock_code, "etype": cfg["event_type"]}).fetchone()
                count = row[0] or 0

            elif cfg.get("event_type_any"):
                row = sess.execute(text("""
                    SELECT COUNT(*) FROM biz.corporate_event WHERE stock_code = :code
                """), {"code": stock_code}).fetchone()
                count = row[0] or 0

            elif cfg.get("doc_type_generic"):
                # 其他公告 = 全部公告 - 已被归类的报告类
                row = sess.execute(text("""
                    SELECT COUNT(*) FROM biz.doc_source_entry
                    WHERE stock_code = :code AND doc_type = 'announcement'
                """), {"code": stock_code}).fetchone()
                count = row[0] or 0

            status, count = _calc_item_status(count, max(threshold, 1))
            completeness[key] = {
                "status": status,
                "count": count,
                "label": cfg["label"],
                "category": cfg["category"],
                "threshold": threshold,
            }

        # 3. 汇总统计
        total_docs = sess.execute(text("""
            SELECT COUNT(*) FROM biz.doc_source_entry WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()[0] or 0

        downloaded_docs = sess.execute(text("""
            SELECT COUNT(*) FROM biz.doc_source_entry
            WHERE stock_code = :code AND is_downloaded = TRUE
        """), {"code": stock_code}).fetchone()[0] or 0

        total_events = sess.execute(text("""
            SELECT COUNT(*) FROM biz.corporate_event WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()[0] or 0

        latest_report = sess.execute(text("""
            SELECT report_date FROM biz.finance_snapshot
            WHERE stock_code = :code ORDER BY report_date DESC LIMIT 1
        """), {"code": stock_code}).fetchone()

        # 4. 写入/更新 research_notebook
        c_obj = NotebookCompleteness(items=completeness)
        score = c_obj.overall_score()

        existing = sess.execute(text("""
            SELECT 1 FROM biz.research_notebook WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()

        if existing:
            sess.execute(text("""
                UPDATE biz.research_notebook SET
                    stock_name = :name,
                    industry_l1 = :l1,
                    industry_l2 = :l2,
                    completeness_json = CAST(:cjson AS jsonb),
                    total_docs = :td,
                    downloaded_docs = :dd,
                    total_events = :te,
                    latest_report_date = :lr,
                    updated_at = NOW()
                WHERE stock_code = :code
            """), {
                "code": stock_code,
                "name": stock.stock_name,
                "l1": stock.primary_industry_l1,
                "l2": stock.primary_industry_l2,
                "cjson": c_obj.to_json(),
                "td": total_docs,
                "dd": downloaded_docs,
                "te": total_events,
                "lr": latest_report[0] if latest_report else None,
            })
        else:
            sess.execute(text("""
                INSERT INTO biz.research_notebook
                    (stock_code, stock_name, industry_l1, industry_l2,
                     completeness_json, total_docs, downloaded_docs,
                     total_events, latest_report_date)
                VALUES
                    (:code, :name, :l1, :l2, CAST(:cjson AS jsonb), :td, :dd, :te, :lr)
            """), {
                "code": stock_code,
                "name": stock.stock_name,
                "l1": stock.primary_industry_l1,
                "l2": stock.primary_industry_l2,
                "cjson": c_obj.to_json(),
                "td": total_docs,
                "dd": downloaded_docs,
                "te": total_events,
                "lr": latest_report[0] if latest_report else None,
            })

    return {
        "stock_code": stock_code,
        "stock_name": stock.stock_name,
        "completeness_score": score,
        "total_docs": total_docs,
        "downloaded_docs": downloaded_docs,
        "total_events": total_events,
    }


def build_all_notebooks(limit: int = 0) -> int:
    """
    批量刷新所有股票的投研笔记本。
    返回处理数量。
    """
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT stock_code FROM core.stock
            WHERE is_delisted = FALSE
            ORDER BY stock_code
        """)).fetchall()
        codes = [r[0] for r in rows]

    if limit and limit > 0:
        codes = codes[:limit]

    logger.info(f"开始构建投研笔记本: {len(codes)} 只")
    count = 0

    for i, code in enumerate(codes, 1):
        try:
            build_notebook_for_stock(code)
            count += 1
        except Exception as e:
            logger.warning(f"{code} notebook 构建失败: {e}")

        if i % 500 == 0:
            logger.info(f"notebook 进度: {i}/{len(codes)}")

    logger.info(f"投研笔记本构建完成: {count} 只")
    return count


def get_notebook_summary(stock_code: str) -> Optional[dict]:
    """获取某只股票的 notebook 概览。"""
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT stock_code, stock_name, industry_l1, industry_l2,
                   completeness_json, total_docs, downloaded_docs,
                   total_events, latest_report_date, thesis, rating, tags,
                   updated_at
            FROM biz.research_notebook WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()

    if not row:
        return None

    c_json = row.completeness_json or {}
    score = NotebookCompleteness(items=c_json).overall_score() if isinstance(c_json, dict) else 0

    return {
        "stock_code": row.stock_code,
        "stock_name": row.stock_name,
        "industry_l1": row.industry_l1,
        "industry_l2": row.industry_l2,
        "completeness_score": score,
        "completeness": c_json,
        "total_docs": row.total_docs,
        "downloaded_docs": row.downloaded_docs,
        "total_events": row.total_events,
        "latest_report_date": row.latest_report_date,
        "thesis": row.thesis,
        "rating": row.rating,
        "tags": row.tags,
        "updated_at": row.updated_at,
    }


def list_missing_items(stock_code: str) -> list[dict]:
    """列出某只股票缺失的资料项（missing + partial）。"""
    nb = get_notebook_summary(stock_code)
    if not nb:
        return []
    c = nb.get("completeness", {})
    return [
        {"key": k, **v}
        for k, v in c.items()
        if v.get("status") in ("missing", "partial")
    ]


def run_build_notebooks(limit: int = 0) -> None:
    """执行全量笔记本构建。"""
    run = start_run(platform_code="local", phase="phase_d_notebook")
    try:
        # 检查上游依赖：core.stock 是否有数据
        stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可构建笔记本",
            )
            logger.warning("笔记本构建跳过：core.stock 为空")
            return

        count = build_all_notebooks(limit=limit)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=count,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=count, error_msg=err_msg)
        if status != "success":
            logger.warning(f"笔记本构建结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"笔记本构建失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_build_notebooks()
