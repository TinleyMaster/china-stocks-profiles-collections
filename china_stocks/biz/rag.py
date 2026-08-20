"""
RAG 检索器 + 问答流程。

两级检索策略（对齐 crypto 项目的"严格依据资料库回答，强制 [编号] 标注来源"）：
  1. 关键词检索 — 用 PostgreSQL 全文搜索/ILIKE，从 doc_chunk 或 doc_source_entry 标题中搜
  2. 向量检索 — 用 pgvector 相似度搜索（需已向量化）

目前先实现"标题级 + 结构化数据"的检索：
  - 从 biz.doc_source_entry 按标题 + content_topics 检索文档
  - 从 biz.corporate_event 检索事件
  - 从 biz.stock_basic / finance_snapshot / capital_snapshot 取画像数据（作为 context）

未来可以扩展到正文级的向量检索（需要先做文档解析 + 切块 + 向量化）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from .llm_client import chat_completion, is_available


@dataclass
class RetrievedDoc:
    """检索到的文档片段。"""
    doc_id: int
    title: str
    source_platform: str
    doc_type: str
    publish_date: Optional[str]
    url: str
    snippet: str = ""
    score: float = 0.0

    def to_dict(self) -> dict:
        return {
            "doc_id": self.doc_id,
            "title": self.title,
            "source": self.source_platform,
            "doc_type": self.doc_type,
            "publish_date": self.publish_date,
            "url": self.url,
            "snippet": self.snippet,
            "score": round(self.score, 3),
        }


def keyword_search_docs(
    stock_code: str,
    query: str,
    doc_types: Optional[list[str]] = None,
    limit: int = 20,
) -> list[RetrievedDoc]:
    """
    按关键词在文档标题中检索。
    用 ILIKE 做简单匹配，排序按发布日期倒序。
    """
    keywords = [kw.strip() for kw in query.split() if kw.strip()]
    if not keywords:
        return []

    sql = """
        SELECT id, title, source_platform, doc_type, publish_date, url
        FROM biz.doc_source_entry
        WHERE stock_code = :code
    """
    params: dict = {"code": stock_code, "limit": limit}

    # 每个关键词做 ILIKE 匹配，全部命中
    for i, kw in enumerate(keywords):
        sql += f" AND title ILIKE :kw{i}"
        params[f"kw{i}"] = f"%{kw}%"

    if doc_types:
        sql += " AND doc_type = ANY(:dtypes)"
        params["dtypes"] = doc_types

    sql += " ORDER BY publish_date DESC LIMIT :limit"

    with get_session() as sess:
        rows = sess.execute(text(sql), params).fetchall()

    results = []
    for r in rows:
        # 简单的相关性得分：标题越短、关键词越靠前得分越高
        score = min(1.0, len(keywords) * 0.3)
        results.append(RetrievedDoc(
            doc_id=r.id,
            title=r.title,
            source_platform=r.source_platform,
            doc_type=r.doc_type,
            publish_date=str(r.publish_date) if r.publish_date else None,
            url=r.url or "",
            snippet=r.title,  # 暂时用标题当 snippet
            score=score,
        ))

    return results


def search_events(
    stock_code: str,
    query: str,
    limit: int = 10,
) -> list[dict]:
    """检索公司事件（按事件类型关键词匹配）。"""
    # 事件类型关键词映射
    type_kw = {
        "dividend": ["分红", "派息", "送转", "利润分配"],
        "unlock": ["解禁", "限售"],
        "buyback": ["回购"],
        "profit_alert": ["业绩预告", "盈利预测", "预增", "预减"],
        "share_change": ["增持", "减持", "举牌"],
    }

    matched_types = []
    for etype, kws in type_kw.items():
        if any(kw in query for kw in kws):
            matched_types.append(etype)

    if not matched_types:
        return []

    sql = """
        SELECT id, event_type, event_date, event_data
        FROM biz.corporate_event
        WHERE stock_code = :code AND event_type = ANY(:types)
        ORDER BY event_date DESC LIMIT :limit
    """
    with get_session() as sess:
        rows = sess.execute(text(sql), {
            "code": stock_code,
            "types": matched_types,
            "limit": limit,
        }).fetchall()

    return [
        {
            "event_id": r.id,
            "event_type": r.event_type,
            "event_date": str(r.event_date) if r.event_date else None,
            "event_data": r.event_data,
        }
        for r in rows
    ]


def get_stock_profile_context(stock_code: str) -> str:
    """获取股票画像文本（作为 RAG context 的一部分）。"""
    with get_session() as sess:
        basic = sess.execute(text("""
            SELECT stock_name, close, change_pct, total_market_cap, pe_ttm, pb,
                   turnover_rate, as_of_date
            FROM biz.stock_basic WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()

        finance = sess.execute(text("""
            SELECT report_date, revenue, revenue_yoy, net_profit, net_profit_yoy,
                   roe, gross_margin, net_margin, debt_ratio, eps, bps
            FROM biz.finance_snapshot WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()

        capital = sess.execute(text("""
            SELECT north_hold_pct, margin_balance
            FROM biz.capital_snapshot WHERE stock_code = :code
        """), {"code": stock_code}).fetchone()

    parts = []

    if basic:
        parts.append(
            f"【行情估值】{basic.stock_name}（{stock_code}）\n"
            f"收盘价: {basic.close} 元（涨跌: {basic.change_pct}%）\n"
            f"总市值: {_fmt_money(basic.total_market_cap)}元\n"
            f"PE(TTM): {basic.pe_ttm}  PB: {basic.pb}\n"
            f"换手率: {basic.turnover_rate}%  截至: {basic.as_of_date}"
        )

    if finance:
        parts.append(
            f"【财务指标】报告期: {finance.report_date}\n"
            f"营收: {_fmt_money(finance.revenue)}元（同比 {finance.revenue_yoy}%）\n"
            f"净利: {_fmt_money(finance.net_profit)}元（同比 {finance.net_profit_yoy}%）\n"
            f"ROE: {finance.roe}%  毛利率: {finance.gross_margin}%  净利率: {finance.net_margin}%\n"
            f"负债率: {finance.debt_ratio}%  EPS: {finance.eps}  BPS: {finance.bps}"
        )

    if capital and (capital.north_hold_pct or capital.margin_balance):
        parts.append(
            f"【资金面】北向持股占比: {capital.north_hold_pct}%\n"
            f"融资余额: {_fmt_money(capital.margin_balance)}元"
        )

    return "\n\n".join(parts) if parts else "暂无画像数据"


def _fmt_money(v) -> str:
    """金额格式化，自动选单位。"""
    if v is None:
        return "N/A"
    try:
        num = float(v)
    except (ValueError, TypeError):
        return str(v)
    if abs(num) >= 1e12:
        return f"{num / 1e12:.2f}万亿"
    if abs(num) >= 1e8:
        return f"{num / 1e8:.2f}亿"
    if abs(num) >= 1e4:
        return f"{num / 1e4:.2f}万"
    return f"{num:.2f}"


# ============================================================
# RAG 问答主流程
# ============================================================

RAG_SYSTEM_PROMPT = """你是一个专业的 A 股投研助手。请严格依据提供的参考资料回答用户问题。

规则：
1. 只能使用给定的参考资料内容，禁止编造信息
2. 回答中引用的每一条信息，都必须在句末标注来源编号，格式为 [编号]
3. 如果资料中没有答案，直接说明"根据现有资料无法回答该问题"，不要猜测
4. 保持客观专业的投资分析语气，不做任何投资建议
5. 最后列出所有引用来源的标题和链接

参考资料：
{context}
"""


def build_context(stock_code: str, query: str, max_docs: int = 15) -> tuple[str, list[RetrievedDoc], list[dict]]:
    """
    构建 RAG 上下文。
    返回 (context_text, docs, events)
    """
    # 1. 文档检索
    docs = keyword_search_docs(stock_code, query, limit=max_docs)

    # 2. 事件检索
    events = search_events(stock_code, query, limit=5)

    # 3. 画像数据
    profile = get_stock_profile_context(stock_code)

    # 组织 context
    context_parts = [f"=== 股票画像 ===\n{profile}"]

    if docs:
        doc_lines = []
        for i, doc in enumerate(docs, 1):
            doc_lines.append(
                f"[{i}] 标题: {doc.title}\n"
                f"    类型: {doc.doc_type}  日期: {doc.publish_date}\n"
                f"    来源: {doc.source_platform}  链接: {doc.url}\n"
            )
        context_parts.append(f"=== 相关文档 ({len(docs)} 条) ===\n" + "\n".join(doc_lines))

    if events:
        evt_lines = []
        for i, evt in enumerate(events, 1):
            evt_lines.append(
                f"[E{i}] 类型: {evt['event_type']}  日期: {evt['event_date']}\n"
                f"     详情: {json.dumps(evt['event_data'], ensure_ascii=False)[:300]}\n"
            )
        context_parts.append(f"=== 相关事件 ({len(events)} 条) ===\n" + "\n".join(evt_lines))

    return "\n\n".join(context_parts), docs, events


def ask_stock(
    stock_code: str,
    question: str,
    save_to_history: bool = True,
) -> dict:
    """
    对单只股票提问（RAG 问答）。

    Args:
        stock_code: 股票代码
        question: 用户问题
        save_to_history: 是否保存到对话历史

    Returns:
        {
            "answer": str,
            "sources": [RetrievedDoc...],
            "events": [...],
            "model": str,
            "usage": dict,
        }
    """
    # 1. 构建上下文
    context, docs, events = build_context(stock_code, question)

    # 2. 组装 prompt
    system_prompt = RAG_SYSTEM_PROMPT.format(context=context)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": question},
    ]

    # 3. 调用 LLM
    try:
        resp = chat_completion(messages, temperature=0.3, max_tokens=2048)
    except Exception as e:
        logger.error(f"RAG 问答失败: {e}")
        return {
            "answer": f"回答生成失败: {e}",
            "sources": [d.to_dict() for d in docs],
            "events": events,
            "model": "error",
            "usage": {},
        }

    answer = resp["content"]
    model = resp["model"]
    usage = resp["usage"]

    # 4. 保存对话历史
    if save_to_history:
        with get_session() as sess:
            # 用户消息
            sess.execute(text("""
                INSERT INTO biz.research_message
                    (stock_code, role, content)
                VALUES (:code, 'user', :content)
            """), {"code": stock_code, "content": question})

            # 助手消息（含来源）
            sources_json = json.dumps([d.to_dict() for d in docs], ensure_ascii=False)
            sess.execute(text("""
                INSERT INTO biz.research_message
                    (stock_code, role, content, sources, model, tokens_used)
                VALUES (:code, 'assistant', :content, :sources::jsonb, :model, :tokens)
            """), {
                "code": stock_code,
                "content": answer,
                "sources": sources_json,
                "model": model,
                "tokens": usage.get("total_tokens", 0),
            })

            # 更新 notebook last_qa_at
            sess.execute(text("""
                UPDATE biz.research_notebook SET last_qa_at = NOW()
                WHERE stock_code = :code
            """), {"code": stock_code})

    return {
        "answer": answer,
        "sources": [d.to_dict() for d in docs],
        "events": events,
        "model": model,
        "usage": usage,
    }


def get_chat_history(stock_code: str, limit: int = 50) -> list[dict]:
    """获取某只股票的对话历史。"""
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT id, role, content, sources, model, tokens_used, created_at
            FROM biz.research_message
            WHERE stock_code = :code
            ORDER BY created_at DESC
            LIMIT :limit
        """), {"code": stock_code, "limit": limit}).fetchall()

    return [
        {
            "id": r.id,
            "role": r.role,
            "content": r.content,
            "sources": r.sources or [],
            "model": r.model,
            "tokens_used": r.tokens_used,
            "created_at": r.created_at,
        }
        for r in reversed(rows)  # 按时间正序返回
    ]
