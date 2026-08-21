"""
Web 工作台 — Flask 轻量 Web 界面。

设计原则：
  - 单文件后端 + 内嵌前端（HTML/JS/CSS 直接从字符串渲染）
  - 零构建、零额外依赖（除了 Flask）
  - 和调度器同进程，用线程启动，共享数据库连接
  - 端口复用 8080，取代之前的 health_server（健康检查合并进来）

页面：
  /                   — 仪表盘（总览）
  /stock/<code>       — 股票详情（画像 + 文档 + 笔记本）
  /docs               — 文档检索
  /ask                — RAG 问答
  /health             — 健康检查（Zeabur 探活用）
  /api/...            — JSON API
"""

from __future__ import annotations

import json
import os
from threading import Thread
from typing import Optional

from flask import Flask, jsonify, render_template_string, request
from sqlalchemy import text

from .config import WEB_HOST, WEB_PORT
from .db import get_session
from .logging_setup import logger


# ============================================================
# Flask App
# ============================================================

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # 中文正常显示


# ============================================================
# API: 仪表盘
# ============================================================


@app.route("/api/stats")
def api_stats():
    """仪表盘统计数据。"""
    with get_session() as sess:
        # 股票总数
        total_stocks = (
            sess.execute(text("SELECT COUNT(*) FROM core.stock")).scalar() or 0
        )

        # 文档总数
        total_docs = (
            sess.execute(text("SELECT COUNT(*) FROM biz.doc_source_entry")).scalar()
            or 0
        )

        # 已下载文档数
        downloaded_docs = (
            sess.execute(
                text(
                    "SELECT COUNT(*) FROM biz.doc_source_entry WHERE is_downloaded = TRUE"
                )
            ).scalar()
            or 0
        )

        # 切块总数
        total_chunks = (
            sess.execute(text("SELECT COUNT(*) FROM biz.doc_chunk")).scalar() or 0
        )

        # 笔记本数
        total_notebooks = (
            sess.execute(text("SELECT COUNT(*) FROM biz.research_notebook")).scalar()
            or 0
        )

        # 最近 10 条采集记录
        recent_runs = sess.execute(
            text("""
            SELECT run_id, phase, status, rows_inserted, rows_updated,
                   started_at, cost_seconds, error_msg
            FROM sys.ingest_run
            ORDER BY run_id DESC
            LIMIT 10
        """)
        ).fetchall()

        # 各文档类型数量
        doc_type_stats = sess.execute(
            text("""
            SELECT doc_type, COUNT(*) as cnt
            FROM biz.doc_source_entry
            GROUP BY doc_type
            ORDER BY cnt DESC
        """)
        ).fetchall()

    return jsonify(
        {
            "total_stocks": total_stocks,
            "total_docs": total_docs,
            "downloaded_docs": downloaded_docs,
            "total_chunks": total_chunks,
            "total_notebooks": total_notebooks,
            "recent_runs": [
                {
                    "run_id": r.run_id,
                    "phase": r.phase,
                    "status": r.status,
                    "rows_inserted": r.rows_inserted or 0,
                    "rows_updated": r.rows_updated or 0,
                    "started_at": str(r.started_at) if r.started_at else None,
                    "cost_seconds": float(r.cost_seconds) if r.cost_seconds else 0,
                    "error_msg": r.error_msg,
                }
                for r in recent_runs
            ],
            "doc_type_stats": [
                {"doc_type": r.doc_type, "count": r.cnt} for r in doc_type_stats
            ],
        }
    )


# ============================================================
# API: 行业列表
# ============================================================


@app.route("/api/industries")
def api_industries():
    """获取申万行业列表（一级 + 二级树状结构）。"""
    with get_session() as sess:
        rows = sess.execute(
            text("""
            SELECT primary_industry_l1 AS industry_l1, primary_industry_l2 AS industry_l2, COUNT(*) as stock_count
            FROM core.stock
            WHERE primary_industry_l1 IS NOT NULL AND primary_industry_l2 IS NOT NULL
            GROUP BY primary_industry_l1, primary_industry_l2
            ORDER BY primary_industry_l1, primary_industry_l2
        """)
        ).fetchall()

    industries = {}
    for r in rows:
        if r.industry_l1 not in industries:
            industries[r.industry_l1] = []
        industries[r.industry_l1].append(
            {
                "name": r.industry_l2,
                "stock_count": r.stock_count,
            }
        )

    return jsonify(
        [{"name": l1, "children": children} for l1, children in industries.items()]
    )


# ============================================================
# API: 股票筛选器
# ============================================================


@app.route("/api/screener")
def api_screener():
    """
    股票筛选器，支持多条件：
      - industry_l1: 申万一级行业
      - industry_l2: 申万二级行业
      - min_cap / max_cap: 市值范围（亿元）
      - min_pe / max_pe: PE(TTM) 范围
      - min_pb / max_pb: PB 范围
      - min_change / max_change: 涨跌幅范围（%）
      - min_roe / max_roe: ROE 范围（%）
      - min_turnover / max_turnover: 换手率范围（%）
      - sort_by: 排序字段（market_cap / pe / pb / change_pct / roe / turnover）
      - sort_order: asc / desc
      - page / page_size: 分页
    """
    # 解析参数
    industry_l1 = request.args.get("industry_l1", "").strip() or None
    industry_l2 = request.args.get("industry_l2", "").strip() or None
    min_cap = request.args.get("min_cap", type=float)
    max_cap = request.args.get("max_cap", type=float)
    min_pe = request.args.get("min_pe", type=float)
    max_pe = request.args.get("max_pe", type=float)
    min_pb = request.args.get("min_pb", type=float)
    max_pb = request.args.get("max_pb", type=float)
    min_change = request.args.get("min_change", type=float)
    max_change = request.args.get("max_change", type=float)
    min_roe = request.args.get("min_roe", type=float)
    max_roe = request.args.get("max_roe", type=float)
    min_turnover = request.args.get("min_turnover", type=float)
    max_turnover = request.args.get("max_turnover", type=float)
    sort_by = request.args.get("sort_by", "market_cap")
    sort_order = request.args.get("sort_order", "desc")
    page = max(1, request.args.get("page", 1, type=int))
    page_size = min(100, request.args.get("page_size", 30, type=int))
    offset = (page - 1) * page_size

    # 允许的排序字段
    allowed_sorts = {
        "market_cap": "b.total_market_cap",
        "pe": "b.pe_ttm",
        "pb": "b.pb",
        "change_pct": "b.change_pct",
        "roe": "f.roe",
        "turnover": "b.turnover_rate",
    }
    sort_expr = allowed_sorts.get(sort_by, "b.total_market_cap")
    sort_order_sql = "ASC" if sort_order == "asc" else "DESC"

    # 构建 WHERE 条件
    conditions = []
    params = {}

    if industry_l1:
        conditions.append("s.primary_industry_l1 = :ind1")
        params["ind1"] = industry_l1
    if industry_l2:
        conditions.append("s.primary_industry_l2 = :ind2")
        params["ind2"] = industry_l2
    if min_cap is not None:
        conditions.append("b.total_market_cap >= :min_cap * 1e8")
        params["min_cap"] = min_cap
    if max_cap is not None:
        conditions.append("b.total_market_cap <= :max_cap * 1e8")
        params["max_cap"] = max_cap
    if min_pe is not None:
        conditions.append("b.pe_ttm >= :min_pe AND b.pe_ttm > 0")
        params["min_pe"] = min_pe
    if max_pe is not None:
        conditions.append("b.pe_ttm <= :max_pe AND b.pe_ttm > 0")
        params["max_pe"] = max_pe
    if min_pb is not None:
        conditions.append("b.pb >= :min_pb AND b.pb > 0")
        params["min_pb"] = min_pb
    if max_pb is not None:
        conditions.append("b.pb <= :max_pb AND b.pb > 0")
        params["max_pb"] = max_pb
    if min_change is not None:
        conditions.append("b.change_pct >= :min_change")
        params["min_change"] = min_change
    if max_change is not None:
        conditions.append("b.change_pct <= :max_change")
        params["max_change"] = max_change
    if min_roe is not None:
        conditions.append("f.roe >= :min_roe")
        params["min_roe"] = min_roe
    if max_roe is not None:
        conditions.append("f.roe <= :max_roe")
        params["max_roe"] = max_roe
    if min_turnover is not None:
        conditions.append("b.turnover_rate >= :min_turnover")
        params["min_turnover"] = min_turnover
    if max_turnover is not None:
        conditions.append("b.turnover_rate <= :max_turnover")
        params["max_turnover"] = max_turnover

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    # 总数
    count_sql = f"""
        SELECT COUNT(*)
        FROM core.stock s
        LEFT JOIN biz.stock_basic b ON b.stock_code = s.stock_code
        LEFT JOIN biz.finance_snapshot f ON f.stock_code = s.stock_code
        {where_sql}
    """

    # 查询数据
    data_sql = f"""
        SELECT s.stock_code, s.stock_name,
               s.primary_industry_l1 AS industry_l1,
               s.primary_industry_l2 AS industry_l2,
               b.close, b.change_pct, b.total_market_cap, b.pe_ttm, b.pb,
               b.turnover_rate, f.roe, f.revenue_yoy, f.net_profit_yoy
        FROM core.stock s
        LEFT JOIN biz.stock_basic b ON b.stock_code = s.stock_code
        LEFT JOIN biz.finance_snapshot f ON f.stock_code = s.stock_code
        {where_sql}
        ORDER BY {sort_expr} {sort_order_sql} NULLS LAST
        LIMIT :page_size OFFSET :offset
    """
    params["page_size"] = page_size
    params["offset"] = offset

    with get_session() as sess:
        total = sess.execute(text(count_sql), params).scalar() or 0
        rows = sess.execute(text(data_sql), params).fetchall()

    results = []
    for r in rows:
        results.append(
            {
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "industry_l1": r.industry_l1,
                "industry_l2": r.industry_l2,
                "close": float(r.close) if r.close else None,
                "change_pct": float(r.change_pct) if r.change_pct else None,
                "total_market_cap": float(r.total_market_cap)
                if r.total_market_cap
                else None,
                "pe_ttm": float(r.pe_ttm) if r.pe_ttm else None,
                "pb": float(r.pb) if r.pb else None,
                "turnover_rate": float(r.turnover_rate) if r.turnover_rate else None,
                "roe": float(r.roe) if r.roe else None,
                "revenue_yoy": float(r.revenue_yoy) if r.revenue_yoy else None,
                "net_profit_yoy": float(r.net_profit_yoy) if r.net_profit_yoy else None,
            }
        )

    return jsonify(
        {
            "results": results,
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    )


# ============================================================
# API: 股票搜索 + 详情
# ============================================================


@app.route("/api/stocks/search")
def api_stock_search():
    """搜索股票（代码或名称）。"""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])

    sql = """
        SELECT stock_code, stock_name,
               primary_industry_l1 AS industry_l1,
               primary_industry_l2 AS industry_l2
        FROM core.stock
        WHERE stock_code ILIKE :q OR stock_name ILIKE :q
        ORDER BY stock_code
        LIMIT 20
    """
    with get_session() as sess:
        rows = sess.execute(text(sql), {"q": f"%{q}%"}).fetchall()

    return jsonify(
        [
            {
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "industry_l1": r.industry_l1,
                "industry_l2": r.industry_l2,
            }
            for r in rows
        ]
    )


@app.route("/api/stock/<code>")
def api_stock_detail(code):
    """股票详情：基本信息 + 估值 + 财务 + 资金面。"""
    code = code.zfill(6)

    with get_session() as sess:
        # 基本信息
        stock = sess.execute(
            text("""
            SELECT stock_code, stock_name,
                   primary_industry_l1 AS industry_l1,
                   primary_industry_l2 AS industry_l2,
                   list_date
            FROM core.stock WHERE stock_code = :code
        """),
            {"code": code},
        ).fetchone()

        if not stock:
            return jsonify({"error": "股票不存在"}), 404

        # 估值画像
        basic = sess.execute(
            text("""
            SELECT close, change_pct, total_market_cap, pe_ttm, pb,
                   turnover_rate, as_of_date
            FROM biz.stock_basic WHERE stock_code = :code
        """),
            {"code": code},
        ).fetchone()

        # 财务
        finance = sess.execute(
            text("""
            SELECT report_date, revenue, revenue_yoy, net_profit, net_profit_yoy,
                   roe, gross_margin, net_margin, debt_ratio, eps, bps
            FROM biz.finance_snapshot WHERE stock_code = :code
        """),
            {"code": code},
        ).fetchone()

        # 资金面
        capital = sess.execute(
            text("""
            SELECT north_hold_pct, margin_balance, as_of_date
            FROM biz.capital_snapshot WHERE stock_code = :code
        """),
            {"code": code},
        ).fetchone()

        # 文档统计
        doc_stats = sess.execute(
            text("""
            SELECT doc_type, COUNT(*) as cnt
            FROM biz.doc_source_entry
            WHERE stock_code = :code
            GROUP BY doc_type
            ORDER BY cnt DESC
        """),
            {"code": code},
        ).fetchall()

    return jsonify(
        {
            "stock": {
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "industry_l1": stock.industry_l1,
                "industry_l2": stock.industry_l2,
                "list_date": str(stock.list_date) if stock.list_date else None,
            },
            "basic": {
                "close": float(basic.close) if basic and basic.close else None,
                "change_pct": float(basic.change_pct)
                if basic and basic.change_pct
                else None,
                "total_market_cap": float(basic.total_market_cap)
                if basic and basic.total_market_cap
                else None,
                "pe_ttm": float(basic.pe_ttm) if basic and basic.pe_ttm else None,
                "pb": float(basic.pb) if basic and basic.pb else None,
                "turnover_rate": float(basic.turnover_rate)
                if basic and basic.turnover_rate
                else None,
                "as_of_date": str(basic.as_of_date)
                if basic and basic.as_of_date
                else None,
            }
            if basic
            else None,
            "finance": {
                "report_date": str(finance.report_date)
                if finance and finance.report_date
                else None,
                "revenue": float(finance.revenue)
                if finance and finance.revenue
                else None,
                "revenue_yoy": float(finance.revenue_yoy)
                if finance and finance.revenue_yoy
                else None,
                "net_profit": float(finance.net_profit)
                if finance and finance.net_profit
                else None,
                "net_profit_yoy": float(finance.net_profit_yoy)
                if finance and finance.net_profit_yoy
                else None,
                "roe": float(finance.roe) if finance and finance.roe else None,
                "gross_margin": float(finance.gross_margin)
                if finance and finance.gross_margin
                else None,
                "net_margin": float(finance.net_margin)
                if finance and finance.net_margin
                else None,
                "debt_ratio": float(finance.debt_ratio)
                if finance and finance.debt_ratio
                else None,
                "eps": float(finance.eps) if finance and finance.eps else None,
                "bps": float(finance.bps) if finance and finance.bps else None,
            }
            if finance
            else None,
            "capital": {
                "north_hold_pct": float(capital.north_hold_pct)
                if capital and capital.north_hold_pct
                else None,
                "margin_balance": float(capital.margin_balance)
                if capital and capital.margin_balance
                else None,
                "as_of_date": str(capital.as_of_date)
                if capital and capital.as_of_date
                else None,
            }
            if capital
            else None,
            "doc_stats": [{"doc_type": r.doc_type, "count": r.cnt} for r in doc_stats],
        }
    )


# ============================================================
# API: K线数据
# ============================================================


@app.route("/api/stock/<code>/kline")
def api_stock_kline(code):
    """
    获取股票日线K线数据。
    支持 ?period=  参数：
      - period: 天数，默认 120 天
    返回 ECharts 友好格式：[[日期, 开, 收, 低, 高, 成交量, ...], ...]
    """
    code = code.zfill(6)
    period = int(request.args.get("period", 120))
    if period > 1000:
        period = 1000

    with get_session() as sess:
        rows = sess.execute(
            text("""
            SELECT trade_date, open, high, low, close, volume, amount,
                   change_pct, turnover_rate
            FROM src_akshare.stock_daily
            WHERE stock_code = :code
            ORDER BY trade_date DESC
            LIMIT :period
        """),
            {"code": code, "period": period},
        ).fetchall()

    if not rows:
        return jsonify({"code": code, "data": [], "count": 0})

    # 按日期升序排列（ECharts 需要）
    rows = list(reversed(rows))

    # 计算 MA5/MA10/MA20
    closes = [float(r.close) for r in rows if r.close]
    ma5 = _calc_ma(closes, 5)
    ma10 = _calc_ma(closes, 10)
    ma20 = _calc_ma(closes, 20)

    data = []
    for i, r in enumerate(rows):
        data.append(
            [
                str(r.trade_date),  # 0: 日期
                float(r.open) if r.open else None,  # 1: 开盘
                float(r.close) if r.close else None,  # 2: 收盘
                float(r.low) if r.low else None,  # 3: 最低
                float(r.high) if r.high else None,  # 4: 最高
                int(r.volume) if r.volume else 0,  # 5: 成交量（手）
                float(r.amount) if r.amount else None,  # 6: 成交额（元）
                float(r.change_pct) if r.change_pct else None,  # 7: 涨跌幅
                ma5[i] if i < len(ma5) else None,  # 8: MA5
                ma10[i] if i < len(ma10) else None,  # 9: MA10
                ma20[i] if i < len(ma20) else None,  # 10: MA20
            ]
        )

    return jsonify(
        {
            "code": code,
            "period": period,
            "count": len(data),
            "data": data,
        }
    )


def _calc_ma(values: list[float], period: int) -> list[Optional[float]]:
    """计算移动平均线。"""
    result = []
    for i in range(len(values)):
        if i < period - 1:
            result.append(None)
        else:
            avg = sum(values[i - period + 1 : i + 1]) / period
            result.append(round(avg, 4))
    return result


# ============================================================
# API: 文档检索
# ============================================================


@app.route("/api/docs/search")
def api_docs_search():
    """文档检索（标题 + 正文双路）。"""
    from .biz.rag import hybrid_search

    code = request.args.get("code", "").strip()
    q = request.args.get("q", "").strip()
    limit = int(request.args.get("limit", 20))

    if not code or not q:
        return jsonify({"results": [], "total": 0})

    results = hybrid_search(code.zfill(6), q, limit=limit)

    return jsonify(
        {
            "results": [r.to_dict() for r in results],
            "total": len(results),
        }
    )


@app.route("/api/docs/list")
def api_docs_list():
    """某只股票的文档列表。"""
    code = request.args.get("code", "").strip().zfill(6)
    doc_type = request.args.get("type", "").strip()
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 20))
    offset = (page - 1) * page_size

    if not code:
        return jsonify({"results": [], "total": 0})

    sql_where = "WHERE stock_code = :code"
    params = {"code": code, "offset": offset, "page_size": page_size}

    if doc_type:
        sql_where += " AND doc_type = :dtype"
        params["dtype"] = doc_type

    with get_session() as sess:
        total = (
            sess.execute(
                text(f"SELECT COUNT(*) FROM biz.doc_source_entry {sql_where}"),
                params,
            ).scalar()
            or 0
        )

        rows = sess.execute(
            text(f"""
            SELECT id, title, doc_type, sub_type, publish_date, url,
                   is_downloaded, source_platform, content_topics
            FROM biz.doc_source_entry
            {sql_where}
            ORDER BY publish_date DESC
            LIMIT :page_size OFFSET :offset
        """),
            params,
        ).fetchall()

    return jsonify(
        {
            "results": [
                {
                    "id": r.id,
                    "title": r.title,
                    "doc_type": r.doc_type,
                    "sub_type": r.sub_type,
                    "publish_date": str(r.publish_date) if r.publish_date else None,
                    "url": r.url,
                    "is_downloaded": r.is_downloaded,
                    "source_platform": r.source_platform,
                    "content_topics": list(r.content_topics)
                    if r.content_topics
                    else [],
                }
                for r in rows
            ],
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    )


# ============================================================
# API: RAG 问答
# ============================================================


@app.route("/api/ask", methods=["POST"])
def api_ask():
    """RAG 问答。"""
    from .biz.rag import ask_stock

    data = request.get_json() or {}
    code = data.get("code", "").strip().zfill(6)
    question = data.get("question", "").strip()

    if not code or not question:
        return jsonify({"error": "缺少 code 或 question 参数"}), 400

    try:
        result = ask_stock(code, question, save_to_history=True)
        return jsonify(result)
    except Exception as e:
        logger.exception(f"RAG 问答失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat/<code>")
def api_chat_history(code):
    """对话历史。"""
    from .biz.rag import get_chat_history

    code = code.zfill(6)
    limit = int(request.args.get("limit", 50))
    history = get_chat_history(code, limit=limit)
    return jsonify(history)


# ============================================================
# API: 投研笔记本
# ============================================================


@app.route("/api/notebook/<code>")
def api_notebook(code):
    """笔记本概览。"""
    from .biz.research_notebook import get_notebook_summary

    code = code.zfill(6)
    nb = get_notebook_summary(code)
    if not nb:
        return jsonify({"error": "笔记本不存在"}), 404
    return jsonify(nb)


# ============================================================
# API: 自选股
# ============================================================


@app.route("/api/watchlist")
def api_watchlist():
    """获取自选股列表（带行情数据）。"""
    with get_session() as sess:
        rows = sess.execute(
            text("""
            SELECT w.stock_code, w.stock_name, w.note, w.tags, w.added_at,
                   s.primary_industry_l1 AS industry_l1,
                   s.primary_industry_l2 AS industry_l2,
                   b.close, b.change_pct, b.total_market_cap, b.pe_ttm
            FROM biz.watchlist w
            LEFT JOIN core.stock s ON s.stock_code = w.stock_code
            LEFT JOIN biz.stock_basic b ON b.stock_code = w.stock_code
            ORDER BY w.added_at DESC
        """)
        ).fetchall()

    return jsonify(
        [
            {
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "note": r.note,
                "tags": list(r.tags) if r.tags else [],
                "added_at": str(r.added_at) if r.added_at else None,
                "industry_l1": r.industry_l1,
                "industry_l2": r.industry_l2,
                "close": float(r.close) if r.close else None,
                "change_pct": float(r.change_pct) if r.change_pct else None,
                "total_market_cap": float(r.total_market_cap)
                if r.total_market_cap
                else None,
                "pe_ttm": float(r.pe_ttm) if r.pe_ttm else None,
            }
            for r in rows
        ]
    )


@app.route("/api/watchlist", methods=["POST"])
def api_watchlist_add():
    """添加自选股。"""
    data = request.get_json() or {}
    code = data.get("code", "").strip().zfill(6)
    if not code:
        return jsonify({"error": "缺少股票代码"}), 400

    with get_session() as sess:
        # 取股票名
        stock = sess.execute(
            text("SELECT stock_name FROM core.stock WHERE stock_code = :code"),
            {"code": code},
        ).fetchone()

        name = stock.stock_name if stock else data.get("name", code)

        sess.execute(
            text("""
            INSERT INTO biz.watchlist (stock_code, stock_name, note)
            VALUES (:code, :name, :note)
            ON CONFLICT (stock_code) DO NOTHING
        """),
            {
                "code": code,
                "name": name,
                "note": data.get("note", ""),
            },
        )

    return jsonify({"ok": True, "stock_code": code, "stock_name": name})


@app.route("/api/watchlist/<code>", methods=["DELETE"])
def api_watchlist_remove(code):
    """移除自选股。"""
    code = code.zfill(6)
    with get_session() as sess:
        sess.execute(
            text("DELETE FROM biz.watchlist WHERE stock_code = :code"),
            {"code": code},
        )
    return jsonify({"ok": True})


@app.route("/api/watchlist/<code>/check")
def api_watchlist_check(code):
    """检查是否在自选股中。"""
    code = code.zfill(6)
    with get_session() as sess:
        row = sess.execute(
            text("SELECT 1 FROM biz.watchlist WHERE stock_code = :code"),
            {"code": code},
        ).fetchone()
    return jsonify({"in_watchlist": row is not None})


# ============================================================
# API: 手动触发采集任务
# ============================================================


@app.route("/api/tasks/list")
def api_tasks_list():
    """列出可手动触发的采集任务。"""
    tasks = [
        {
            "id": "phase_a",
            "name": "Phase A: 股票池刷新",
            "desc": "全 A 股列表 + 申万行业",
        },
        {
            "id": "phase_daily",
            "name": "Phase B: 日线行情采集",
            "desc": "增量更新日线数据",
        },
        {
            "id": "stock_basic",
            "name": "Phase C: 估值画像",
            "desc": "stock_basic 表刷新",
        },
        {
            "id": "finance",
            "name": "Phase C: 财务指标",
            "desc": "finance_snapshot 表刷新",
        },
        {"id": "capital", "name": "Phase C: 资金面画像", "desc": "北向 + 融资融券"},
        {
            "id": "shareholder",
            "name": "Phase C: 股东画像",
            "desc": "十大股东 + 质押 + 户数",
        },
        {
            "id": "announcements",
            "name": "Phase B2: 公告入口",
            "desc": "巨潮资讯网公告抓取",
        },
        {"id": "research", "name": "Phase B3: 券商研报", "desc": "东财研报中心抓取"},
        {"id": "survey", "name": "Phase B3: 调研纪要", "desc": "机构调研纪要抓取"},
        {"id": "download_docs", "name": "Phase B2: 文档下载", "desc": "批量下载 PDF"},
        {
            "id": "parse_docs",
            "name": "Phase B: 文档解析切块",
            "desc": "PDF → doc_chunk",
        },
        {
            "id": "events",
            "name": "Phase D: 公司事件",
            "desc": "分红/解禁/业绩预告/回购",
        },
        {"id": "notebook", "name": "Phase D: 笔记本刷新", "desc": "完整性清单更新"},
    ]
    return jsonify(tasks)


@app.route("/api/tasks/trigger", methods=["POST"])
def api_trigger_task():
    """
    手动触发采集任务（异步，启动后立即返回）。
    用后台线程运行，避免阻塞请求。
    """
    data = request.get_json() or {}
    task_id = data.get("task_id", "")

    # 任务映射
    task_map = {
        "phase_a": (
            "Phase A-股票池构建",
            lambda: __import__(
                "china_stocks.src.phase_a_stock_pool", fromlist=["run_phase_a"]
            ).run_phase_a(),
        ),
        "phase_daily": (
            "Phase B-日线行情",
            lambda: __import__(
                "china_stocks.src.phase_b_daily", fromlist=["run_phase_daily"]
            ).run_phase_daily(),
        ),
        "stock_basic": (
            "Phase C-估值画像",
            lambda: __import__(
                "china_stocks.biz.stock_basic", fromlist=["run_stock_basic"]
            ).run_stock_basic(),
        ),
        "finance": (
            "Phase C-财务指标",
            lambda: __import__(
                "china_stocks.biz.finance_snapshot", fromlist=["run_finance_snapshot"]
            ).run_finance_snapshot(),
        ),
        "capital": (
            "Phase C-资金面",
            lambda: __import__(
                "china_stocks.biz.capital_snapshot", fromlist=["run_capital_snapshot"]
            ).run_capital_snapshot(),
        ),
        "shareholder": (
            "Phase C-股东画像",
            lambda: __import__(
                "china_stocks.biz.shareholder_snapshot",
                fromlist=["run_shareholder_snapshot"],
            ).run_shareholder_snapshot(),
        ),
        "announcements": (
            "Phase B2-公告入口",
            lambda: __import__(
                "china_stocks.src.phase_b2_announcements",
                fromlist=["run_phase_b2_announcements"],
            ).run_phase_b2_announcements(),
        ),
        "research": (
            "Phase B3-券商研报",
            lambda: __import__(
                "china_stocks.src.phase_b3_research", fromlist=["run_phase_b3_research"]
            ).run_phase_b3_research(),
        ),
        "survey": (
            "Phase B3-调研纪要",
            lambda: __import__(
                "china_stocks.src.phase_b3_survey", fromlist=["run_phase_b3_survey"]
            ).run_phase_b3_survey(),
        ),
        "download_docs": (
            "Phase B2-文档下载",
            lambda: __import__(
                "china_stocks.src.phase_b2_download",
                fromlist=["run_download_announcements"],
            ).run_download_announcements(),
        ),
        "parse_docs": (
            "Phase B-文档解析",
            lambda: __import__(
                "china_stocks.biz.doc_parser", fromlist=["run_parse_docs"]
            ).run_parse_docs(),
        ),
        "events": (
            "Phase D-公司事件",
            lambda: __import__(
                "china_stocks.src.phase_d_events", fromlist=["run_corporate_events"]
            ).run_corporate_events(),
        ),
        "notebook": (
            "Phase D-笔记本刷新",
            lambda: __import__(
                "china_stocks.biz.research_notebook", fromlist=["run_build_notebooks"]
            ).run_build_notebooks(),
        ),
    }

    if task_id not in task_map:
        return jsonify({"error": f"未知任务: {task_id}"}), 400

    task_name, func = task_map[task_id]

    # 后台线程运行
    import threading

    def _run():
        try:
            logger.info(f"[手动触发] 开始: {task_name}")
            func()
            logger.info(f"[手动触发] 完成: {task_name}")
        except Exception as e:
            logger.exception(f"[手动触发] 失败: {task_name} - {e}")

    t = threading.Thread(target=_run, daemon=True, name=f"manual-{task_id}")
    t.start()

    logger.info(f"手动触发任务: {task_name}")
    return jsonify(
        {"ok": True, "task_id": task_id, "task_name": task_name, "status": "running"}
    )


# ============================================================
# 健康检查
# ============================================================


@app.route("/health")
def health():
    """健康检查端点（Zeabur 探活用）。"""
    try:
        with get_session() as sess:
            sess.execute(text("SELECT 1"))
        return jsonify({"status": "ok", "db": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "db": str(e)}), 500


@app.route("/api/status")
def api_status():
    """详细状态（和旧 health_server 兼容）。"""
    try:
        with get_session() as sess:
            sess.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"

    return jsonify(
        {
            "status": "running",
            "db": db_status,
            "version": "1.0.0",
        }
    )


# ============================================================
# 前端页面（内嵌 HTML）
# ============================================================


@app.route("/")
def index():
    return render_template_string(INDEX_HTML)


# ============================================================
# 启动函数
# ============================================================


def start_web_server(host: str = "0.0.0.0", port: int = 8080) -> Thread:
    """
    在后台线程启动 Flask Web 服务。
    取代旧的 health_server，提供完整 Web 工作台 + 健康检查。

    注意：如果 Web 服务启动失败（端口被占等），会记录错误但不影响主调度器。
    """

    def _run():
        try:
            # 生产环境关闭 debug，不用 reloader（避免子进程重复启动调度器）
            app.run(
                host=host,
                port=port,
                debug=False,
                use_reloader=False,
                threaded=True,
            )
        except Exception as e:
            logger.error(f"Web 服务启动失败: {e}", exc_info=True)
            # 给调度器发信号，让主进程也退出（避免 Zeabur 显示假活 502）
            import os

            os._exit(1)

    t = Thread(target=_run, daemon=True, name="web-server")
    t.start()
    # 给一点启动时间
    import time

    time.sleep(0.5)
    if not t.is_alive():
        logger.error("Web 线程启动后立即退出，服务不可用")
    else:
        logger.info(f"Web 工作台启动在 http://{host}:{port}")
    return t


# ============================================================
# 前端 HTML（内嵌，避免静态文件管理麻烦）
# ============================================================

INDEX_HTML = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>A股投研工作台</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; }
    .nav-item.active { background-color: #1e40af; color: white; }
    .card { background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; }
    .tag-success { background: #dcfce7; color: #166534; }
    .tag-warning { background: #fef3c7; color: #92400e; }
    .tag-error { background: #fee2e2; color: #991b1b; }
    .tag-info { background: #dbeafe; color: #1e40af; }
    .tag-gray { background: #f3f4f6; color: #4b5563; }
    .loading { display: inline-block; width: 16px; height: 16px; border: 2px solid #e5e7eb; border-top-color: #3b82f6; border-radius: 50%; animation: spin 0.8s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .scrollbar-thin::-webkit-scrollbar { width: 6px; height: 6px; }
    .scrollbar-thin::-webkit-scrollbar-thumb { background: #d1d5db; border-radius: 3px; }
    .chat-bubble-user { background: #1e40af; color: white; border-radius: 12px 12px 2px 12px; }
    .chat-bubble-assistant { background: #f3f4f6; color: #111827; border-radius: 12px 12px 12px 2px; }
  </style>
</head>
<body class="bg-gray-50 min-h-screen flex">

  <!-- 侧边栏 -->
  <aside class="w-56 bg-gray-900 text-white min-h-screen flex flex-col">
    <div class="p-4 border-b border-gray-700">
      <h1 class="text-lg font-bold">📈 A股投研工作台</h1>
      <p class="text-xs text-gray-400 mt-1">china-stocks-profiles</p>
    </div>
    <nav class="flex-1 p-2 space-y-1">
      <a href="#" class="nav-item active block px-4 py-2 rounded text-sm" data-page="dashboard">📊 仪表盘</a>
      <a href="#" class="nav-item block px-4 py-2 rounded text-sm text-gray-300 hover:bg-gray-800" data-page="screener">🔍 选股</a>
      <a href="#" class="nav-item block px-4 py-2 rounded text-sm text-gray-300 hover:bg-gray-800" data-page="watchlist">⭐ 自选股</a>
      <a href="#" class="nav-item block px-4 py-2 rounded text-sm text-gray-300 hover:bg-gray-800" data-page="docs">📄 文档检索</a>
      <a href="#" class="nav-item block px-4 py-2 rounded text-sm text-gray-300 hover:bg-gray-800" data-page="ask">💬 智能问答</a>
      <a href="#" class="nav-item block px-4 py-2 rounded text-sm text-gray-300 hover:bg-gray-800" data-page="notebook">📓 投研笔记本</a>
      <a href="#" class="nav-item block px-4 py-2 rounded text-sm text-gray-300 hover:bg-gray-800" data-page="tasks">⚙️ 采集任务</a>
    </nav>
    <div class="p-4 border-t border-gray-700">
      <div class="flex items-center gap-2">
        <span class="w-2 h-2 bg-green-400 rounded-full"></span>
        <span class="text-xs text-gray-400" id="db-status">数据库连接中...</span>
      </div>
    </div>
  </aside>

  <!-- 主内容区 -->
  <main class="flex-1 p-6 overflow-auto">
    <div id="page-content"></div>
  </main>

  <!-- 股票搜索模态框 -->
  <div id="stock-modal" class="fixed inset-0 bg-black/50 hidden items-center justify-center z-50">
    <div class="bg-white rounded-lg p-6 w-[500px] max-h-[80vh] overflow-auto">
      <div class="flex justify-between items-center mb-4">
        <h3 class="font-bold text-lg">搜索股票</h3>
        <button onclick="closeStockModal()" class="text-gray-400 hover:text-gray-600 text-2xl">&times;</button>
      </div>
      <input type="text" id="stock-search-input" placeholder="输入股票代码或名称..." 
        class="w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500">
      <div id="stock-search-results" class="mt-4 space-y-2 max-h-96 overflow-auto scrollbar-thin"></div>
    </div>
  </div>

<script>
// ============================================================
// 路由 + 页面切换
// ============================================================
const pages = {
  dashboard: renderDashboard,
  screener: renderScreener,
  watchlist: renderWatchlist,
  docs: renderDocs,
  ask: renderAsk,
  notebook: renderNotebook,
  stock: renderStockDetail,
  tasks: renderTasks,
};

let currentPage = 'dashboard';
let currentStock = null;  // {stock_code, stock_name}

document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', (e) => {
    e.preventDefault();
    const page = item.dataset.page;
    navigateTo(page);
  });
});

function navigateTo(page, params = {}) {
  currentPage = page;
  // 子页面对应的主导航高亮
  const navMap = {
    stock: 'dashboard',    // 股票详情 → 仪表盘（无专门入口）
    docs: 'docs',
    ask: 'ask',
    notebook: 'notebook',
    screener: 'screener',
    watchlist: 'watchlist',
    tasks: 'tasks',
    dashboard: 'dashboard',
  };
  const activeNav = navMap[page] || 'dashboard';
  document.querySelectorAll('.nav-item').forEach(el => {
    const isActive = el.dataset.page === activeNav;
    el.classList.toggle('active', isActive);
    if (isActive) {
      el.classList.remove('text-gray-300', 'hover:bg-gray-800');
      el.classList.add('bg-blue-700', 'text-white');
    } else {
      el.classList.add('text-gray-300', 'hover:bg-gray-800');
      el.classList.remove('bg-blue-700', 'text-white');
    }
  });
  (pages[page] || renderDashboard)(params);
}

// ============================================================
// 工具函数
// ============================================================
async function api(path, options = {}) {
  try {
    const resp = await fetch(path, {
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      ...options,
    });
    if (!resp.ok) {
      console.error(`API 错误 ${resp.status}: ${path}`);
      try {
        const err = await resp.json();
        return { error: err.error || `HTTP ${resp.status}` };
      } catch {
        return { error: `HTTP ${resp.status}` };
      }
    }
    return await resp.json();
  } catch (e) {
    console.error(`网络错误: ${path}`, e);
    return { error: e.message || '网络错误' };
  }
}

function fmtMoney(v) {
  if (v === null || v === undefined) return 'N/A';
  const num = Number(v);
  if (Math.abs(num) >= 1e12) return (num / 1e12).toFixed(2) + '万亿';
  if (Math.abs(num) >= 1e8) return (num / 1e8).toFixed(2) + '亿';
  if (Math.abs(num) >= 1e4) return (num / 1e4).toFixed(2) + '万';
  return num.toFixed(2);
}

function fmtPct(v) {
  if (v === null || v === undefined) return 'N/A';
  return Number(v).toFixed(2) + '%';
}

function statusTag(status) {
  const map = { success: 'tag-success', running: 'tag-info', failed: 'tag-error' };
  const cls = map[status] || 'tag-gray';
  return `<span class="tag ${cls}">${status}</span>`;
}

function docTypeLabel(type) {
  const map = {
    announcement: '公告', annual_report: '年报', semi_annual_report: '半年报',
    quarterly_report: '季报', research: '研报', survey: '调研纪要',
    prospectus: '招股书', listing: '上市公告', other: '其他',
  };
  return map[type] || type;
}

// ============================================================
// 仪表盘
// ============================================================
async function renderDashboard() {
  const content = document.getElementById('page-content');
  content.innerHTML = '<div class="flex items-center gap-2"><span class="loading"></span> 加载中...</div>';

  const [data, watchlist] = await Promise.all([
    api('/api/stats'),
    api('/api/watchlist'),
  ]);

  content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">仪表盘</h2>

    <!-- 统计卡片 -->
    <div class="grid grid-cols-4 gap-4 mb-8">
      <div class="card p-5">
        <div class="text-gray-500 text-sm">股票总数</div>
        <div class="text-3xl font-bold mt-2">${data.total_stocks.toLocaleString()}</div>
        <div class="text-xs text-gray-400 mt-1">A股上市公司</div>
      </div>
      <div class="card p-5">
        <div class="text-gray-500 text-sm">文档总数</div>
        <div class="text-3xl font-bold mt-2">${data.total_docs.toLocaleString()}</div>
        <div class="text-xs text-gray-400 mt-1">已下载 ${data.downloaded_docs.toLocaleString()} 篇</div>
      </div>
      <div class="card p-5">
        <div class="text-gray-500 text-sm">文本切块</div>
        <div class="text-3xl font-bold mt-2">${data.total_chunks.toLocaleString()}</div>
        <div class="text-xs text-gray-400 mt-1">RAG 检索用</div>
      </div>
      <div class="card p-5">
        <div class="text-gray-500 text-sm">投研笔记本</div>
        <div class="text-3xl font-bold mt-2">${data.total_notebooks.toLocaleString()}</div>
        <div class="text-xs text-gray-400 mt-1">完整性清单</div>
      </div>
    </div>

    <div class="grid grid-cols-2 gap-6">
      <!-- 最近采集任务 -->
      <div class="card p-5">
        <h3 class="font-bold mb-4">最近采集任务</h3>
        <div class="space-y-2">
          ${data.recent_runs.map(r => `
            <div class="flex items-center justify-between p-3 bg-gray-50 rounded">
              <div>
                <div class="font-medium text-sm">${r.phase}</div>
                <div class="text-xs text-gray-400 mt-1">${r.started_at ? r.started_at.slice(0, 19) : '-'} · 耗时 ${r.cost_seconds?.toFixed(1) || 0}s</div>
              </div>
              <div class="text-right">
                ${statusTag(r.status)}
                <div class="text-xs text-gray-400 mt-1">+${r.rows_inserted} / ${r.rows_updated}</div>
              </div>
            </div>
          `).join('')}
        </div>
      </div>

      <!-- 文档类型分布 -->
      <div class="card p-5">
        <h3 class="font-bold mb-4">文档类型分布</h3>
        <div class="space-y-3">
          ${data.doc_type_stats.map(d => {
            const max = Math.max(...data.doc_type_stats.map(x => x.count));
            const pct = (d.count / max * 100).toFixed(0);
            return `
              <div>
                <div class="flex justify-between text-sm mb-1">
                  <span>${docTypeLabel(d.doc_type)}</span>
                  <span class="text-gray-500">${d.count}</span>
                </div>
                <div class="h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div class="h-full bg-blue-500 rounded-full" style="width: ${pct}%"></div>
                </div>
              </div>
            `;
          }).join('')}
        </div>
      </div>
    </div>

    <!-- 自选股行情 -->
    <div class="card p-5 mt-6">
      <div class="flex justify-between items-center mb-4">
        <h3 class="font-bold">⭐ 自选股行情</h3>
        <a href="#" onclick="navigateTo('watchlist'); return false;" class="text-sm text-blue-600 hover:underline">查看全部 →</a>
      </div>
      ${watchlist.length === 0
        ? `<div class="text-gray-400 text-center py-6 text-sm">
            还没有自选股 ·
            <a href="#" onclick="openStockModal(); return false;" class="text-blue-600 hover:underline">去添加</a>
          </div>`
        : `<div class="grid grid-cols-5 gap-3">
            ${watchlist.slice(0, 5).map(s => `
              <div onclick="selectStock('${s.stock_code}', '${s.stock_name}')"
                class="p-3 border rounded hover:border-blue-400 cursor-pointer transition-colors">
                <div class="font-medium text-sm truncate">${s.stock_name}</div>
                <div class="text-xs text-gray-400 mb-2">${s.stock_code}</div>
                <div class="text-lg font-bold">${s.close ?? 'N/A'}</div>
                <div class="text-xs ${s.change_pct >= 0 ? 'text-red-500' : 'text-green-500'}">
                  ${s.change_pct !== null && s.change_pct !== undefined ? (s.change_pct > 0 ? '+' : '') + s.change_pct.toFixed(2) + '%' : 'N/A'}
                </div>
              </div>
            `).join('')}
          </div>`
      }
    </div>

    <!-- 快速操作 -->
    <div class="card p-5 mt-6">
      <h3 class="font-bold mb-4">快速操作</h3>
      <div class="flex gap-4">
        <button onclick="openStockModal()" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
          🔍 搜索股票
        </button>
        <button onclick="navigateTo('ask')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
          💬 智能问答
        </button>
        <button onclick="navigateTo('docs')" class="px-4 py-2 bg-purple-600 text-white rounded hover:bg-purple-700">
          📄 文档检索
        </button>
      </div>
    </div>
  `;
}

// ============================================================
// 股票搜索模态框
// ============================================================
function openStockModal() {
  document.getElementById('stock-modal').classList.remove('hidden');
  document.getElementById('stock-modal').classList.add('flex');
  document.getElementById('stock-search-input').focus();
  document.getElementById('stock-search-results').innerHTML = '';
}

function closeStockModal() {
  document.getElementById('stock-modal').classList.add('hidden');
  document.getElementById('stock-modal').classList.remove('flex');
}

let searchTimer = null;
document.getElementById('stock-search-input').addEventListener('input', (e) => {
  const q = e.target.value.trim();
  clearTimeout(searchTimer);
  if (q.length < 1) {
    document.getElementById('stock-search-results').innerHTML = '';
    return;
  }
  searchTimer = setTimeout(async () => {
    const results = await api(`/api/stocks/search?q=${encodeURIComponent(q)}`);
    document.getElementById('stock-search-results').innerHTML = results.length
      ? results.map(s => `
        <div class="p-3 hover:bg-gray-50 rounded cursor-pointer flex justify-between items-center border-b last:border-0"
          onclick="selectStock('${s.stock_code}', '${s.stock_name}')">
          <div>
            <span class="font-medium">${s.stock_name}</span>
            <span class="text-gray-400 ml-2">${s.stock_code}</span>
          </div>
          <span class="text-xs text-gray-400">${s.industry_l1 || ''} · ${s.industry_l2 || ''}</span>
        </div>
      `).join('')
      : '<div class="text-gray-400 text-center py-4">没有找到相关股票</div>';
  }, 200);
});

function selectStock(code, name) {
  currentStock = { stock_code: code, stock_name: name };
  closeStockModal();
  navigateTo('stock', { code });
}

// 点击遮罩关闭
document.getElementById('stock-modal').addEventListener('click', (e) => {
  if (e.target.id === 'stock-modal') closeStockModal();
});

// ============================================================
// 股票详情
// ============================================================
async function renderStockDetail(params) {
  const code = params.code || (currentStock && currentStock.stock_code);
  if (!code) { openStockModal(); return; }

  const content = document.getElementById('page-content');
  content.innerHTML = '<div class="flex items-center gap-2"><span class="loading"></span> 加载中...</div>';

  const data = await api(`/api/stock/${code}`);
  if (data.error) { content.innerHTML = `<div class="text-red-500">${data.error}</div>`; return; }

  // 检查是否在自选股中
  const check = await api(`/api/watchlist/${code}/check`);
  const inWatchlist = check.in_watchlist;

  const s = data.stock;
  const b = data.basic || {};
  const f = data.finance || {};
  const c = data.capital || {};

  content.innerHTML = `
    <div class="flex items-center justify-between mb-6">
      <div>
        <h2 class="text-2xl font-bold">${s.stock_name} <span class="text-gray-400 font-normal text-lg">${s.stock_code}</span></h2>
        <p class="text-gray-500 text-sm mt-1">${s.industry_l1 || ''} · ${s.industry_l2 || ''} · 上市日期: ${s.list_date || 'N/A'}</p>
      </div>
      <div class="flex gap-2">
        <button id="watchlist-btn" onclick="${inWatchlist
          ? `removeFromWatchlist('${s.stock_code}')`
          : `addToWatchlist('${s.stock_code}', '${s.stock_name}')`
        }" class="${inWatchlist
          ? 'px-4 py-2 bg-yellow-500 text-white rounded hover:bg-yellow-600'
          : 'px-4 py-2 border rounded hover:bg-gray-50'
        }">${inWatchlist ? '⭐ 已收藏' : '☆ 收藏'}</button>
        <button onclick="navigateTo('docs', {code: '${s.stock_code}'})" class="px-4 py-2 border rounded hover:bg-gray-50">📄 文档</button>
        <button onclick="navigateTo('ask', {code: '${s.stock_code}'})" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">💬 问答</button>
        <button onclick="navigateTo('notebook', {code: '${s.stock_code}'})" class="px-4 py-2 border rounded hover:bg-gray-50">📓 笔记本</button>
      </div>
    </div>

    <!-- 估值行情 -->
    <div class="card p-5 mb-6">
      <h3 class="font-bold mb-4">行情估值</h3>
      <div class="grid grid-cols-6 gap-4">
        <div>
          <div class="text-gray-500 text-sm">收盘价</div>
          <div class="text-xl font-bold mt-1">${b.close ?? 'N/A'}</div>
        </div>
        <div>
          <div class="text-gray-500 text-sm">涨跌幅</div>
          <div class="text-xl font-bold mt-1 ${b.change_pct >= 0 ? 'text-red-500' : 'text-green-500'}">${b.change_pct !== null && b.change_pct !== undefined ? (b.change_pct > 0 ? '+' : '') + b.change_pct.toFixed(2) + '%' : 'N/A'}</div>
        </div>
        <div>
          <div class="text-gray-500 text-sm">总市值</div>
          <div class="text-xl font-bold mt-1">${fmtMoney(b.total_market_cap)}</div>
        </div>
        <div>
          <div class="text-gray-500 text-sm">PE(TTM)</div>
          <div class="text-xl font-bold mt-1">${b.pe_ttm ?? 'N/A'}</div>
        </div>
        <div>
          <div class="text-gray-500 text-sm">PB</div>
          <div class="text-xl font-bold mt-1">${b.pb ?? 'N/A'}</div>
        </div>
        <div>
          <div class="text-gray-500 text-sm">换手率</div>
          <div class="text-xl font-bold mt-1">${fmtPct(b.turnover_rate)}</div>
        </div>
      </div>
      ${b.as_of_date ? `<div class="text-xs text-gray-400 mt-3">数据截至: ${b.as_of_date.slice(0,10)}</div>` : ''}
    </div>

    <!-- K线图 -->
    <div class="card p-5 mb-6">
      <div class="flex justify-between items-center mb-3">
        <h3 class="font-bold">K 线走势</h3>
        <div class="flex gap-1 text-sm">
          <button onclick="switchKlinePeriod(30)" class="px-2 py-1 border rounded hover:bg-gray-50" data-kp="30">30日</button>
          <button onclick="switchKlinePeriod(60)" class="px-2 py-1 border rounded hover:bg-gray-50 bg-blue-50 border-blue-300" data-kp="60">60日</button>
          <button onclick="switchKlinePeriod(120)" class="px-2 py-1 border rounded hover:bg-gray-50" data-kp="120">120日</button>
          <button onclick="switchKlinePeriod(250)" class="px-2 py-1 border rounded hover:bg-gray-50" data-kp="250">年线</button>
        </div>
      </div>
      <div id="kline-chart" style="height: 400px;"></div>
    </div>

    <div class="grid grid-cols-2 gap-6">
      <!-- 财务指标 -->
      <div class="card p-5">
        <h3 class="font-bold mb-4">财务指标 ${f.report_date ? `<span class="text-sm text-gray-400 font-normal">(${f.report_date.slice(0,10)})</span>` : ''}</h3>
        <div class="space-y-3">
          <div class="flex justify-between"><span class="text-gray-500">营业收入</span><span class="font-medium">${fmtMoney(f.revenue)}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">营收同比</span><span class="font-medium ${f.revenue_yoy >= 0 ? 'text-red-500' : 'text-green-500'}">${f.revenue_yoy !== null && f.revenue_yoy !== undefined ? (f.revenue_yoy > 0 ? '+' : '') + f.revenue_yoy.toFixed(2) + '%' : 'N/A'}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">净利润</span><span class="font-medium">${fmtMoney(f.net_profit)}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">净利同比</span><span class="font-medium ${f.net_profit_yoy >= 0 ? 'text-red-500' : 'text-green-500'}">${f.net_profit_yoy !== null && f.net_profit_yoy !== undefined ? (f.net_profit_yoy > 0 ? '+' : '') + f.net_profit_yoy.toFixed(2) + '%' : 'N/A'}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">ROE</span><span class="font-medium">${fmtPct(f.roe)}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">毛利率</span><span class="font-medium">${fmtPct(f.gross_margin)}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">净利率</span><span class="font-medium">${fmtPct(f.net_margin)}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">资产负债率</span><span class="font-medium">${fmtPct(f.debt_ratio)}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">EPS</span><span class="font-medium">${f.eps ?? 'N/A'}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">BPS</span><span class="font-medium">${f.bps ?? 'N/A'}</span></div>
        </div>
      </div>

      <!-- 资金面 + 文档统计 -->
      <div class="space-y-6">
        <div class="card p-5">
          <h3 class="font-bold mb-4">资金面</h3>
          <div class="space-y-3">
            <div class="flex justify-between"><span class="text-gray-500">北向持股占比</span><span class="font-medium">${fmtPct(c.north_hold_pct)}</span></div>
            <div class="flex justify-between"><span class="text-gray-500">融资余额</span><span class="font-medium">${fmtMoney(c.margin_balance)}</span></div>
          </div>
        </div>
        <div class="card p-5">
          <h3 class="font-bold mb-4">文档统计</h3>
          <div class="space-y-2">
            ${data.doc_stats.length
              ? data.doc_stats.map(d => `
                <div class="flex justify-between text-sm">
                  <span>${docTypeLabel(d.doc_type)}</span>
                  <span class="text-gray-500">${d.count} 篇</span>
                </div>
              `).join('')
              : '<div class="text-gray-400 text-sm">暂无文档</div>'
            }
          </div>
        </div>
      </div>
    </div>
  `;

  // 加载K线图（等DOM渲染完）
  setTimeout(() => loadKline(code, 60), 50);
}

// ============================================================
// 文档检索
// ============================================================
async function renderDocs(params = {}) {
  const code = params.code || (currentStock && currentStock.stock_code) || '';
  const content = document.getElementById('page-content');

  content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">文档检索</h2>

    <div class="card p-5 mb-6">
      <div class="flex gap-3">
        <div class="flex-1">
          <label class="text-sm text-gray-500 block mb-1">股票代码</label>
          <input type="text" id="doc-stock-code" value="${code}" placeholder="输入股票代码，如 600519"
            class="w-full px-4 py-2 border rounded focus:outline-none focus:ring-2 focus:ring-blue-500">
        </div>
        <div class="flex-[2]">
          <label class="text-sm text-gray-500 block mb-1">检索关键词</label>
          <input type="text" id="doc-query" placeholder="输入关键词，空格分隔多关键词"
            class="w-full px-4 py-2 border rounded focus:outline-none focus:ring-2 focus:ring-blue-500">
        </div>
        <div class="flex items-end">
          <button onclick="searchDocs()" class="px-6 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
            🔍 检索
          </button>
        </div>
      </div>
    </div>

    <div id="doc-results" class="card p-5">
      <div class="text-gray-400 text-center py-8">输入关键词开始检索</div>
    </div>
  `;
}

async function searchDocs() {
  const code = document.getElementById('doc-stock-code').value.trim().zfill(6);
  const q = document.getElementById('doc-query').value.trim();
  if (!code) { alert('请输入股票代码'); return; }
  if (!q) { alert('请输入检索关键词'); return; }

  currentStock = { stock_code: code, stock_name: code };
  const resultsDiv = document.getElementById('doc-results');
  resultsDiv.innerHTML = '<div class="flex items-center justify-center gap-2 py-8"><span class="loading"></span> 检索中...</div>';

  const data = await api(`/api/docs/search?code=${code}&q=${encodeURIComponent(q)}`);

  if (!data.results || data.results.length === 0) {
    resultsDiv.innerHTML = '<div class="text-gray-400 text-center py-8">没有找到匹配的文档</div>';
    return;
  }

  resultsDiv.innerHTML = `
    <div class="mb-4 text-sm text-gray-500">找到 ${data.total} 条相关结果</div>
    <div class="space-y-4">
      ${data.results.map((d, i) => `
        <div class="p-4 border rounded hover:border-blue-300 transition-colors">
          <div class="flex justify-between items-start">
            <div class="flex-1">
              <div class="font-medium text-blue-600 hover:underline cursor-pointer"
                onclick="window.open('${d.url}', '_blank')">
                ${i + 1}. ${d.title}
                ${d.chunk_index !== null && d.chunk_index !== undefined ? `<span class="text-xs text-gray-400 ml-2">（第 ${d.chunk_index} 段）</span>` : ''}
              </div>
              <div class="flex gap-3 mt-2 text-xs text-gray-500">
                <span class="tag tag-info">${docTypeLabel(d.doc_type)}</span>
                <span>${d.source}</span>
                <span>${d.publish_date || 'N/A'}</span>
                <span>得分: ${d.score}</span>
              </div>
              <div class="mt-3 text-sm text-gray-600 bg-gray-50 p-3 rounded">
                ${d.snippet}
              </div>
            </div>
          </div>
        </div>
      `).join('')}
    </div>
  `;
}

// 回车触发检索
document.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && document.getElementById('doc-query') === document.activeElement) {
    searchDocs();
  }
});

// ============================================================
// RAG 智能问答
// ============================================================
function renderAsk(params = {}) {
  const code = params.code || (currentStock && currentStock.stock_code) || '';
  const content = document.getElementById('page-content');

  content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">智能问答</h2>

    <div class="grid grid-cols-[280px_1fr] gap-6 h-[calc(100vh-120px)]">
      <!-- 左侧：股票选择 + 历史 -->
      <div class="card p-4 flex flex-col">
        <div class="mb-4">
          <label class="text-sm text-gray-500 block mb-1">股票代码</label>
          <input type="text" id="ask-stock-code" value="${code}" placeholder="如 600519"
            class="w-full px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
        </div>
        <div class="text-sm font-medium mb-2 text-gray-700">对话历史</div>
        <div id="chat-history-list" class="flex-1 overflow-auto scrollbar-thin space-y-1 text-sm">
          ${code ? '<div class="text-gray-400 text-center py-2"><span class="loading"></span> 加载中...</div>' : '<div class="text-gray-400 text-center py-2">输入股票代码查看历史</div>'}
        </div>
      </div>

      <!-- 右侧：对话区 -->
      <div class="card flex flex-col h-full">
        <div id="chat-messages" class="flex-1 overflow-auto scrollbar-thin p-5 space-y-4">
          <div class="text-center text-gray-400 py-8">
            <div class="text-4xl mb-3">💬</div>
            <div>输入股票代码和问题，开始智能问答</div>
            <div class="text-xs mt-2">回答严格基于资料库，标注来源</div>
          </div>
        </div>
        <div class="border-t p-4">
          <div class="flex gap-2">
            <textarea id="ask-input" rows="2" placeholder="输入你的问题..."
              class="flex-1 px-4 py-2 border rounded resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"></textarea>
            <button onclick="sendQuestion()" class="px-6 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 self-end">
              发送
            </button>
          </div>
        </div>
      </div>
    </div>
  `;

  if (code) loadChatHistory(code);

  // 回车发送（Ctrl+Enter 换行）
  const textarea = document.getElementById('ask-input');
  textarea.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.ctrlKey && !e.shiftKey) {
      e.preventDefault();
      sendQuestion();
    }
  });
}

async function loadChatHistory(code) {
  const history = await api(`/api/chat/${code}`);
  const list = document.getElementById('chat-history-list');
  if (!history || history.length === 0) {
    list.innerHTML = '<div class="text-gray-400 text-center py-2">暂无对话历史</div>';
    return;
  }
  list.innerHTML = history.slice().reverse().map((m, i) => `
    <div class="p-2 hover:bg-gray-50 rounded cursor-pointer truncate ${i === 0 ? 'bg-blue-50' : ''}"
      onclick="loadConversation(${history.length - 1 - i})">
      ${m.role === 'user' ? '❓ ' : '🤖 '}${m.content.slice(0, 30)}...
    </div>
  `).join('');
}

let conversation = [];

async function sendQuestion() {
  const code = document.getElementById('ask-stock-code').value.trim().zfill(6);
  const question = document.getElementById('ask-input').value.trim();
  if (!code) { alert('请输入股票代码'); return; }
  if (!question) return;

  currentStock = { stock_code: code, stock_name: code };

  // 显示用户消息
  appendMessage('user', question);
  document.getElementById('ask-input').value = '';

  // 显示加载中
  const loadingId = 'loading-' + Date.now();
  appendLoading(loadingId);

  try {
    const resp = await api('/api/ask', {
      method: 'POST',
      body: JSON.stringify({ code, question }),
    });
    removeLoading(loadingId);

    if (resp.error) {
      appendMessage('assistant', `❌ 错误: ${resp.error}`);
    } else {
      appendMessage('assistant', resp.answer, resp.sources);
    }
  } catch (e) {
    removeLoading(loadingId);
    appendMessage('assistant', `❌ 请求失败: ${e.message}`);
  }

  loadChatHistory(code);
}

function appendMessage(role, content, sources = null) {
  const container = document.getElementById('chat-messages');
  // 移除初始占位
  const placeholder = container.querySelector('.text-center.text-gray-400');
  if (placeholder) placeholder.remove();

  const div = document.createElement('div');
  div.className = `flex ${role === 'user' ? 'justify-end' : 'justify-start'}`;

  const sourcesHtml = sources && sources.length
    ? `<div class="mt-2 pt-2 border-t border-gray-200 text-xs text-gray-500">
        <div class="font-medium mb-1">引用来源 (${sources.length}):</div>
        ${sources.map((s, i) => `
          <div>[${i + 1}] ${s.title} · ${docTypeLabel(s.doc_type)} · ${s.publish_date || 'N/A'}</div>
        `).join('')}
      </div>`
    : '';

  div.innerHTML = `
    <div class="${role === 'user' ? 'chat-bubble-user' : 'chat-bubble-assistant'} max-w-[80%] p-3 text-sm whitespace-pre-wrap">
      ${content}
      ${sourcesHtml}
    </div>
  `;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
}

function appendLoading(id) {
  const container = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.id = id;
  div.className = 'flex justify-start';
  div.innerHTML = `<div class="chat-bubble-assistant p-3 text-sm"><span class="loading"></span> 思考中...</div>`;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
}

function removeLoading(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

// ============================================================
// 投研笔记本
// ============================================================
async function renderNotebook(params = {}) {
  const code = params.code || (currentStock && currentStock.stock_code) || '';
  const content = document.getElementById('page-content');

  if (!code) {
    content.innerHTML = `
      <h2 class="text-2xl font-bold mb-6">投研笔记本</h2>
      <div class="card p-8 text-center">
        <div class="text-4xl mb-3">📓</div>
        <div class="text-gray-500 mb-4">请先选择一只股票</div>
        <button onclick="openStockModal()" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
          搜索股票
        </button>
      </div>
    `;
    return;
  }

  content.innerHTML = '<div class="flex items-center gap-2"><span class="loading"></span> 加载中...</div>';

  const data = await api(`/api/notebook/${code}`);
  if (data.error) { content.innerHTML = `<div class="text-red-500">${data.error}</div>`; return; }

  const c = data.completeness || {};
  const categories = {};
  Object.values(c).forEach(item => {
    const cat = item.category || '其他';
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push(item);
  });

  content.innerHTML = `
    <div class="flex items-center justify-between mb-6">
      <div>
        <h2 class="text-2xl font-bold">${data.stock_name} <span class="text-gray-400 font-normal text-lg">${data.stock_code}</span></h2>
        <p class="text-gray-500 text-sm mt-1">${data.industry_l1 || ''} · ${data.industry_l2 || ''}</p>
      </div>
      <div class="flex gap-2">
        <button onclick="navigateTo('stock', {code: '${data.stock_code}'})" class="px-4 py-2 border rounded hover:bg-gray-50">📊 画像</button>
        <button onclick="navigateTo('docs', {code: '${data.stock_code}'})" class="px-4 py-2 border rounded hover:bg-gray-50">📄 文档</button>
      </div>
    </div>

    <!-- 完整度总览 -->
    <div class="card p-6 mb-6">
      <div class="flex items-center justify-between mb-4">
        <h3 class="font-bold text-lg">资料完整度</h3>
        <div class="text-right">
          <div class="text-4xl font-bold ${data.completeness_score >= 80 ? 'text-green-500' : data.completeness_score >= 50 ? 'text-yellow-500' : 'text-red-500'}">
            ${data.completeness_score}<span class="text-lg">/100</span>
          </div>
          <div class="text-xs text-gray-400 mt-1">
            文档 ${data.total_docs} 篇 · 事件 ${data.total_events} 条
          </div>
        </div>
      </div>
      <div class="h-3 bg-gray-100 rounded-full overflow-hidden">
        <div class="h-full ${data.completeness_score >= 80 ? 'bg-green-500' : data.completeness_score >= 50 ? 'bg-yellow-500' : 'bg-red-500'} rounded-full transition-all"
          style="width: ${data.completeness_score}%"></div>
      </div>
    </div>

    <!-- 分类完整度 -->
    <div class="grid grid-cols-2 gap-6">
      ${Object.entries(categories).map(([cat, items]) => {
        const done = items.filter(i => i.status === 'done').length;
        return `
          <div class="card p-5">
            <h3 class="font-bold mb-4 flex justify-between">
              <span>${cat}</span>
              <span class="text-sm text-gray-400">${done}/${items.length}</span>
            </h3>
            <div class="space-y-2">
              ${items.sort((a, b) => a.status === 'done' ? 1 : -1).map(item => {
                const icon = item.status === 'done' ? '✅' : item.status === 'partial' ? '🟡' : '❌';
                return `
                  <div class="flex justify-between items-center text-sm p-2 bg-gray-50 rounded">
                    <span>${icon} ${item.label}</span>
                    <span class="text-gray-400">${item.count} / ${item.threshold}</span>
                  </div>
                `;
              }).join('')}
            </div>
          </div>
        `;
      }).join('')}
    </div>

    ${data.rating ? `
      <div class="card p-5 mt-6">
        <h3 class="font-bold mb-3">自评级</h3>
        <div class="text-2xl font-bold">${data.rating}</div>
      </div>
    ` : ''}
  `;
}

// ============================================================
// K线图
// ============================================================
let klineChart = null;
let currentKlinePeriod = 60;
let currentKlineCode = null;

async function loadKline(code, period = 60) {
  currentKlineCode = code;
  currentKlinePeriod = period;

  const resp = await api(`/api/stock/${code}/kline?period=${period}`);
  if (!resp.data || resp.data.length === 0) {
    const el = document.getElementById('kline-chart');
    if (el) el.innerHTML = '<div class="text-center text-gray-400 py-16">暂无行情数据，先采集日线行情</div>';
    return;
  }

  renderKlineChart(resp.data);
}

function renderKlineChart(data) {
  const dom = document.getElementById('kline-chart');
  if (!dom) return;

  if (!klineChart) {
    klineChart = echarts.init(dom);
    window.addEventListener('resize', () => klineChart && klineChart.resize());
  }

  const dates = data.map(d => d[0]);
  const klineData = data.map(d => [d[1], d[2], d[3], d[4]]);  // 开, 收, 低, 高
  const volumes = data.map((d, i) => {
    // 成交量柱：涨红跌绿
    const color = d[2] >= d[1] ? '#ef4444' : '#10b981';
    return { value: d[5], itemStyle: { color } };
  });
  const ma5 = data.map(d => d[8]);
  const ma10 = data.map(d => d[9]);
  const ma20 = data.map(d => d[10]);

  const option = {
    animation: false,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: 'rgba(255,255,255,0.95)',
      borderColor: '#ddd',
      borderWidth: 1,
      textStyle: { color: '#333', fontSize: 12 },
      formatter: function(params) {
        if (!params || !params.length) return '';
        const idx = params[0].dataIndex;
        const d = data[idx];
        const pct = d[7] !== null ? (d[7] > 0 ? '+' : '') + d[7].toFixed(2) + '%' : 'N/A';
        return `
          <div style="font-weight:bold;margin-bottom:4px">${d[0]}</div>
          开盘: <b>${d[1] ?? 'N/A'}</b><br>
          收盘: <b>${d[2] ?? 'N/A'}</b><br>
          最高: <b>${d[4] ?? 'N/A'}</b><br>
          最低: <b>${d[3] ?? 'N/A'}</b><br>
          涨跌: <b>${pct}</b><br>
          成交量: <b>${(d[5] / 10000).toFixed(2)}万手</b><br>
          成交额: <b>${d[6] ? (d[6] / 100000000).toFixed(2) + '亿' : 'N/A'}</b>
        `;
      }
    },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    grid: [
      { left: '8%', right: '3%', top: '5%', height: '60%' },
      { left: '8%', right: '3%', top: '72%', height: '18%' }
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: false,
        axisLine: { onZero: false },
        axisLabel: { fontSize: 11 },
        splitLine: { show: false },
        min: 'dataMin',
        max: 'dataMax',
      },
      {
        type: 'category',
        gridIndex: 1,
        data: dates,
        boundaryGap: false,
        axisLine: { onZero: false },
        axisTick: { show: false },
        axisLabel: { show: false },
        splitLine: { show: false },
        min: 'dataMin',
        max: 'dataMax',
      }
    ],
    yAxis: [
      {
        scale: true,
        splitArea: { show: true, areaStyle: { color: ['rgba(0,0,0,0.02)', 'rgba(0,0,0,0)'] } },
        axisLabel: { fontSize: 11 },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: { show: false },
        splitLine: { show: false },
      }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 50, end: 100 },
      { show: true, xAxisIndex: [0, 1], type: 'slider', bottom: '2%', height: 18, start: 50, end: 100 }
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: klineData,
        itemStyle: {
          color: '#ef4444',
          color0: '#10b981',
          borderColor: '#ef4444',
          borderColor0: '#10b981',
        },
      },
      {
        name: 'MA5',
        type: 'line',
        data: ma5,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1, color: '#f59e0b' },
      },
      {
        name: 'MA10',
        type: 'line',
        data: ma10,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1, color: '#3b82f6' },
      },
      {
        name: 'MA20',
        type: 'line',
        data: ma20,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1, color: '#a855f7' },
      },
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
      }
    ],
    legend: {
      data: ['K线', 'MA5', 'MA10', 'MA20', '成交量'],
      top: 0,
      right: 0,
      textStyle: { fontSize: 11 },
    },
  };

  klineChart.setOption(option);
}

function switchKlinePeriod(period) {
  if (!currentKlineCode) return;
  // 更新按钮状态
  document.querySelectorAll('[data-kp]').forEach(btn => {
    const p = parseInt(btn.dataset.kp);
    if (p === period) {
      btn.classList.add('bg-blue-50', 'border-blue-300');
    } else {
      btn.classList.remove('bg-blue-50', 'border-blue-300');
    }
  });
  loadKline(currentKlineCode, period);
}

// ============================================================
// 选股页面
// ============================================================
let screenerIndustries = null;
let screenerPage = 1;
let screenerSortBy = 'market_cap';
let screenerSortOrder = 'desc';

async function renderScreener() {
  const content = document.getElementById('page-content');
  content.innerHTML = '<div class="flex items-center gap-2"><span class="loading"></span> 加载中...</div>';

  // 加载行业数据（缓存）
  if (!screenerIndustries) {
    screenerIndustries = await api('/api/industries');
  }

  content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">🔍 股票筛选</h2>

    <!-- 筛选条件 -->
    <div class="card p-5 mb-6">
      <div class="grid grid-cols-4 gap-4">
        <div>
          <label class="text-sm text-gray-500 block mb-1">申万一级行业</label>
          <select id="scr-ind1" onchange="onIndustry1Change()" class="w-full px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <option value="">全部行业</option>
            ${screenerIndustries.map(i => `<option value="${i.name}">${i.name}</option>`).join('')}
          </select>
        </div>
        <div>
          <label class="text-sm text-gray-500 block mb-1">申万二级行业</label>
          <select id="scr-ind2" class="w-full px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <option value="">全部二级行业</option>
          </select>
        </div>
        <div>
          <label class="text-sm text-gray-500 block mb-1">市值（亿元）</label>
          <div class="flex gap-2">
            <input type="number" id="scr-min-cap" placeholder="最小" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <input type="number" id="scr-max-cap" placeholder="最大" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
          </div>
        </div>
        <div>
          <label class="text-sm text-gray-500 block mb-1">PE(TTM)</label>
          <div class="flex gap-2">
            <input type="number" id="scr-min-pe" placeholder="最小" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <input type="number" id="scr-max-pe" placeholder="最大" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
          </div>
        </div>
        <div>
          <label class="text-sm text-gray-500 block mb-1">PB</label>
          <div class="flex gap-2">
            <input type="number" id="scr-min-pb" step="0.1" placeholder="最小" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <input type="number" id="scr-max-pb" step="0.1" placeholder="最大" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
          </div>
        </div>
        <div>
          <label class="text-sm text-gray-500 block mb-1">涨跌幅 (%)</label>
          <div class="flex gap-2">
            <input type="number" id="scr-min-change" step="0.1" placeholder="最小" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <input type="number" id="scr-max-change" step="0.1" placeholder="最大" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
          </div>
        </div>
        <div>
          <label class="text-sm text-gray-500 block mb-1">ROE (%)</label>
          <div class="flex gap-2">
            <input type="number" id="scr-min-roe" step="0.1" placeholder="最小" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <input type="number" id="scr-max-roe" step="0.1" placeholder="最大" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
          </div>
        </div>
        <div>
          <label class="text-sm text-gray-500 block mb-1">换手率 (%)</label>
          <div class="flex gap-2">
            <input type="number" id="scr-min-turn" step="0.1" placeholder="最小" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
            <input type="number" id="scr-max-turn" step="0.1" placeholder="最大" class="flex-1 px-3 py-2 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
          </div>
        </div>
      </div>
      <div class="flex justify-end gap-2 mt-4">
        <button onclick="resetScreener()" class="px-4 py-2 border rounded hover:bg-gray-50">重置</button>
        <button onclick="runScreener()" class="px-6 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">🔍 开始筛选</button>
      </div>
    </div>

    <!-- 结果表格 -->
    <div id="screener-results" class="card">
      <div class="text-center text-gray-400 py-12">设置条件后点击"开始筛选"</div>
    </div>
  `;
}

function onIndustry1Change() {
  const ind1 = document.getElementById('scr-ind1').value;
  const ind2Select = document.getElementById('scr-ind2');
  if (!ind1 || !screenerIndustries) {
    ind2Select.innerHTML = '<option value="">全部二级行业</option>';
    return;
  }
  const industry = screenerIndustries.find(i => i.name === ind1);
  ind2Select.innerHTML = '<option value="">全部二级行业</option>' +
    (industry ? industry.children.map(c => `<option value="${c.name}">${c.name} (${c.stock_count})</option>`).join('') : '');
}

function resetScreener() {
  document.getElementById('scr-ind1').value = '';
  document.getElementById('scr-ind2').innerHTML = '<option value="">全部二级行业</option>';
  ['min-cap', 'max-cap', 'min-pe', 'max-pe', 'min-pb', 'max-pb',
   'min-change', 'max-change', 'min-roe', 'max-roe', 'min-turn', 'max-turn'
  ].forEach(id => { const el = document.getElementById('scr-' + id); if (el) el.value = ''; });
  screenerPage = 1;
  document.getElementById('screener-results').innerHTML =
    '<div class="text-center text-gray-400 py-12">设置条件后点击"开始筛选"</div>';
}

function _getVal(id) {
  const el = document.getElementById(id);
  if (!el) return null;
  const v = el.value.trim();
  return v === '' ? null : Number(v);
}

async function runScreener(page = 1) {
  screenerPage = page;
  const resultsDiv = document.getElementById('screener-results');
  resultsDiv.innerHTML = '<div class="flex justify-center py-12"><span class="loading"></span></div>';

  const params = new URLSearchParams();
  const ind1 = document.getElementById('scr-ind1').value;
  const ind2 = document.getElementById('scr-ind2').value;
  if (ind1) params.set('industry_l1', ind1);
  if (ind2) params.set('industry_l2', ind2);

  const fields = [
    ['min-cap', 'min_cap'], ['max-cap', 'max_cap'],
    ['min-pe', 'min_pe'], ['max-pe', 'max_pe'],
    ['min-pb', 'min_pb'], ['max-pb', 'max_pb'],
    ['min-change', 'min_change'], ['max-change', 'max_change'],
    ['min-roe', 'min_roe'], ['max-roe', 'max_roe'],
    ['min-turn', 'min_turnover'], ['max-turn', 'max_turnover'],
  ];
  fields.forEach(([id, key]) => {
    const v = _getVal('scr-' + id);
    if (v !== null) params.set(key, v);
  });

  params.set('sort_by', screenerSortBy);
  params.set('sort_order', screenerSortOrder);
  params.set('page', page);
  params.set('page_size', 30);

  const data = await api(`/api/screener?${params.toString()}`);
  renderScreenerResults(data);
}

function renderScreenerResults(data) {
  const resultsDiv = document.getElementById('screener-results');
  if (!data.results || data.results.length === 0) {
    resultsDiv.innerHTML = '<div class="text-center text-gray-400 py-12">没有找到符合条件的股票</div>';
    return;
  }

  const totalPages = Math.ceil(data.total / data.page_size);
  const sortCol = (field, label) => {
    const isActive = screenerSortBy === field;
    const arrow = isActive ? (screenerSortOrder === 'desc' ? ' ↓' : ' ↑') : '';
    return `<th class="text-right p-3 text-sm text-gray-600 cursor-pointer hover:bg-gray-50 ${isActive ? 'text-blue-600' : ''}"
      onclick="sortScreener('${field}')">${label}${arrow}</th>`;
  };

  resultsDiv.innerHTML = `
    <div class="p-4 border-b flex justify-between items-center">
      <div class="text-sm text-gray-500">共 <b>${data.total}</b> 只股票符合条件</div>
      <div class="text-xs text-gray-400">点击列标题可排序</div>
    </div>
    <div class="overflow-x-auto">
      <table class="w-full text-sm">
        <thead class="bg-gray-50 border-b sticky top-0">
          <tr>
            <th class="text-left p-3 text-sm text-gray-600">股票</th>
            <th class="text-left p-3 text-sm text-gray-600">行业</th>
            ${sortCol('market_cap', '市值')}
            <th class="text-right p-3 text-sm text-gray-600">收盘价</th>
            ${sortCol('change_pct', '涨跌幅')}
            ${sortCol('pe', 'PE(TTM)')}
            ${sortCol('pb', 'PB')}
            ${sortCol('roe', 'ROE')}
            ${sortCol('turnover', '换手率')}
          </tr>
        </thead>
        <tbody>
          ${data.results.map(s => `
            <tr class="border-b hover:bg-gray-50 cursor-pointer" onclick="selectStock('${s.stock_code}', '${s.stock_name}')">
              <td class="p-3">
                <div class="font-medium">${s.stock_name}</div>
                <div class="text-xs text-gray-400">${s.stock_code}</div>
              </td>
              <td class="p-3 text-xs text-gray-500">
                <div>${s.industry_l1 || ''}</div>
                <div class="text-gray-400">${s.industry_l2 || ''}</div>
              </td>
              <td class="text-right p-3 font-medium">${fmtMoney(s.total_market_cap)}</td>
              <td class="text-right p-3">${s.close ?? 'N/A'}</td>
              <td class="text-right p-3 font-medium ${s.change_pct >= 0 ? 'text-red-500' : 'text-green-500'}">
                ${s.change_pct !== null && s.change_pct !== undefined ? (s.change_pct > 0 ? '+' : '') + s.change_pct.toFixed(2) + '%' : 'N/A'}
              </td>
              <td class="text-right p-3">${s.pe_ttm ?? 'N/A'}</td>
              <td class="text-right p-3">${s.pb ?? 'N/A'}</td>
              <td class="text-right p-3">${s.roe !== null && s.roe !== undefined ? s.roe.toFixed(2) + '%' : 'N/A'}</td>
              <td class="text-right p-3">${s.turnover_rate !== null && s.turnover_rate !== undefined ? s.turnover_rate.toFixed(2) + '%' : 'N/A'}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>

    <!-- 分页 -->
    <div class="p-4 border-t flex justify-center items-center gap-2">
      <button onclick="runScreener(${screenerPage - 1})" ${screenerPage <= 1 ? 'disabled' : ''}
        class="px-3 py-1 border rounded text-sm ${screenerPage <= 1 ? 'text-gray-300 cursor-not-allowed' : 'hover:bg-gray-50'}">
        上一页
      </button>
      <span class="text-sm text-gray-500">第 ${screenerPage} / ${totalPages} 页</span>
      <button onclick="runScreener(${screenerPage + 1})" ${screenerPage >= totalPages ? 'disabled' : ''}
        class="px-3 py-1 border rounded text-sm ${screenerPage >= totalPages ? 'text-gray-300 cursor-not-allowed' : 'hover:bg-gray-50'}">
        下一页
      </button>
    </div>
  `;
}

function sortScreener(field) {
  if (screenerSortBy === field) {
    screenerSortOrder = screenerSortOrder === 'desc' ? 'asc' : 'desc';
  } else {
    screenerSortBy = field;
    screenerSortOrder = 'desc';
  }
  runScreener(screenerPage);
}

// ============================================================
// 自选股页面
// ============================================================
async function renderWatchlist() {
  const content = document.getElementById('page-content');
  content.innerHTML = '<div class="flex items-center gap-2"><span class="loading"></span> 加载中...</div>';

  const data = await api('/api/watchlist');

  content.innerHTML = `
    <div class="flex justify-between items-center mb-6">
      <h2 class="text-2xl font-bold">⭐ 自选股</h2>
      <button onclick="openStockModal()" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
        ➕ 添加自选股
      </button>
    </div>

    ${data.length === 0
      ? `<div class="card p-12 text-center">
          <div class="text-5xl mb-4">📋</div>
          <div class="text-gray-500 mb-4">还没有自选股</div>
          <button onclick="openStockModal()" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
            添加第一只
          </button>
        </div>`
      : `<div class="card overflow-hidden">
          <table class="w-full">
            <thead class="bg-gray-50 border-b">
              <tr>
                <th class="text-left p-3 text-sm text-gray-600">股票</th>
                <th class="text-right p-3 text-sm text-gray-600">最新价</th>
                <th class="text-right p-3 text-sm text-gray-600">涨跌幅</th>
                <th class="text-right p-3 text-sm text-gray-600">市值</th>
                <th class="text-right p-3 text-sm text-gray-600">PE(TTM)</th>
                <th class="text-center p-3 text-sm text-gray-600">操作</th>
              </tr>
            </thead>
            <tbody>
              ${data.map(s => `
                <tr class="border-b hover:bg-gray-50 cursor-pointer" onclick="selectStock('${s.stock_code}', '${s.stock_name}')">
                  <td class="p-3">
                    <div class="font-medium">${s.stock_name}</div>
                    <div class="text-xs text-gray-400">${s.stock_code} · ${s.industry_l1 || ''}</div>
                  </td>
                  <td class="text-right p-3 font-medium">${s.close ?? 'N/A'}</td>
                  <td class="text-right p-3 font-medium ${s.change_pct >= 0 ? 'text-red-500' : 'text-green-500'}">
                    ${s.change_pct !== null && s.change_pct !== undefined ? (s.change_pct > 0 ? '+' : '') + s.change_pct.toFixed(2) + '%' : 'N/A'}
                  </td>
                  <td class="text-right p-3">${fmtMoney(s.total_market_cap)}</td>
                  <td class="text-right p-3">${s.pe_ttm ?? 'N/A'}</td>
                  <td class="text-center p-3">
                    <button onclick="event.stopPropagation(); removeFromWatchlist('${s.stock_code}')"
                      class="text-red-500 hover:text-red-700 text-sm">移除</button>
                  </td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>`
    }
  `;
}

async function addToWatchlist(code, name) {
  const resp = await api('/api/watchlist', {
    method: 'POST',
    body: JSON.stringify({ code, name }),
  });
  if (resp.ok) {
    alert(`已添加 ${name} 到自选股`);
    if (currentPage === 'watchlist') renderWatchlist();
    if (currentPage === 'stock') {
      const btn = document.getElementById('watchlist-btn');
      if (btn) {
        btn.textContent = '⭐ 已收藏';
        btn.className = 'px-4 py-2 bg-yellow-500 text-white rounded hover:bg-yellow-600';
        btn.onclick = () => removeFromWatchlist('${code}');
      }
    }
  }
}

async function removeFromWatchlist(code) {
  if (!confirm('确定要移除这只自选股吗？')) return;
  const resp = await api(`/api/watchlist/${code}`, { method: 'DELETE' });
  if (resp.ok) {
    if (currentPage === 'watchlist') renderWatchlist();
    if (currentPage === 'stock') {
      const btn = document.getElementById('watchlist-btn');
      if (btn) {
        btn.textContent = '☆ 收藏';
        btn.className = 'px-4 py-2 border rounded hover:bg-gray-50';
        btn.onclick = () => addToWatchlist(currentStock.stock_code, currentStock.stock_name);
      }
    }
  }
}

// ============================================================
// 采集任务页面
// ============================================================
async function renderTasks() {
  const content = document.getElementById('page-content');
  content.innerHTML = '<div class="flex items-center gap-2"><span class="loading"></span> 加载中...</div>';

  const [tasks, stats] = await Promise.all([
    api('/api/tasks/list'),
    api('/api/stats'),
  ]);

  content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">⚙️ 采集任务</h2>

    <div class="grid grid-cols-2 gap-6">
      <!-- 任务列表 -->
      <div>
        <h3 class="font-bold mb-4">可执行任务</h3>
        <div class="space-y-2">
          ${tasks.map(t => `
            <div class="card p-4 flex justify-between items-center">
              <div>
                <div class="font-medium">${t.name}</div>
                <div class="text-xs text-gray-400 mt-1">${t.desc}</div>
              </div>
              <button onclick="triggerTask('${t.id}', '${t.name}')"
                class="px-4 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-700">
                ▶ 运行
              </button>
            </div>
          `).join('')}
        </div>
        <div class="text-xs text-gray-400 mt-3">
          💡 提示：任务在后台异步运行，关闭页面不影响执行。可在下方查看最近运行记录。
        </div>
      </div>

      <!-- 最近运行记录 -->
      <div>
        <h3 class="font-bold mb-4">最近运行记录</h3>
        <div class="card p-4">
          <div class="space-y-2">
            ${stats.recent_runs.map(r => `
              <div class="flex items-center justify-between p-3 bg-gray-50 rounded">
                <div>
                  <div class="font-medium text-sm">${r.phase}</div>
                  <div class="text-xs text-gray-400 mt-1">
                    ${r.started_at ? r.started_at.slice(5, 19) : '-'} · 耗时 ${r.cost_seconds?.toFixed(1) || 0}s
                  </div>
                </div>
                <div class="text-right">
                  ${statusTag(r.status)}
                  ${r.error_msg ? `<div class="text-xs text-red-500 mt-1 max-w-[200px] truncate">${r.error_msg}</div>` : ''}
                </div>
              </div>
            `).join('')}
          </div>
        </div>
      </div>
    </div>
  `;
}

async function triggerTask(taskId, taskName) {
  if (!confirm(`确定要手动运行「${taskName}」吗？`)) return;

  const resp = await api('/api/tasks/trigger', {
    method: 'POST',
    body: JSON.stringify({ task_id: taskId }),
  });

  if (resp.ok) {
    alert(`任务已启动：${taskName}\n任务在后台运行，可在下方记录中查看进度。`);
    // 3 秒后刷新记录
    setTimeout(() => { if (currentPage === 'tasks') renderTasks(); }, 3000);
  } else {
    alert(`启动失败：${resp.error}`);
  }
}

// ============================================================
// 初始化
// ============================================================
async function init() {
  // 检查数据库连接
  try {
    const status = await api('/api/status');
    document.getElementById('db-status').textContent =
      status.db === 'ok' ? '数据库已连接' : '数据库异常';
  } catch (e) {
    document.getElementById('db-status').textContent = '连接失败';
  }

  // 渲染初始页面
  navigateTo('dashboard');
}

init();
</script>
</body>
</html>
"""
