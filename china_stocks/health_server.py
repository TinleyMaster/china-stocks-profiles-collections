"""
极简 HTTP 健康检查服务（Zeabur 用）。

为什么需要：
  - Zeabur 默认用 HTTP 探活来判断服务是否正常启动
  - 我们的主服务是 APScheduler 调度器，没有 Web 端口
  - 加一个超轻量的 HTTP 端点，既满足 Zeabur 探活，又能看系统状态

端口：默认 8080（Zeabur 惯例）
端点：
  GET /health   → 200 OK + 基本状态
  GET /status   → 采集任务状态（最近 10 条）
  GET /          → 简单的 welcome 页面
"""
from __future__ import annotations

import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

from .logging_setup import logger


def _get_status_json() -> dict:
    """获取系统状态 JSON。"""
    try:
        from .db import get_session
        from sqlalchemy import text

        with get_session() as sess:
            # 最近采集状态
            rows = sess.execute(text("""
                SELECT run_id, phase, status, rows_inserted, rows_updated,
                       started_at, cost_seconds
                FROM sys.ingest_run
                ORDER BY run_id DESC
                LIMIT 10
            """)).fetchall()

            recent_runs = [
                {
                    "run_id": r.run_id,
                    "phase": r.phase,
                    "status": r.status,
                    "rows_inserted": r.rows_inserted,
                    "rows_updated": r.rows_updated,
                    "started_at": str(r.started_at) if r.started_at else None,
                    "cost_seconds": float(r.cost_seconds) if r.cost_seconds else None,
                }
                for r in rows
            ]

            # 股票数量
            stock_count = sess.execute(
                text("SELECT COUNT(*) FROM core.stock WHERE is_delisted = FALSE")
            ).fetchone()[0]

            # 文档数量
            doc_count = sess.execute(text("SELECT COUNT(*) FROM biz.doc_source_entry")).fetchone()[0]

            # 笔记本数量
            nb_count = sess.execute(text("SELECT COUNT(*) FROM biz.research_notebook")).fetchone()[0]

        return {
            "status": "ok",
            "service": "china-stocks-profiles-collections",
            "version": "0.1.0",
            "stocks": stock_count,
            "documents": doc_count,
            "notebooks": nb_count,
            "recent_runs": recent_runs,
        }
    except Exception as e:
        return {
            "status": "degraded",
            "error": str(e),
            "service": "china-stocks-profiles-collections",
        }


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health" or self.path == "/healthz":
            self._send_json(200, {"status": "ok"})
        elif self.path == "/status":
            self._send_json(200, _get_status_json())
        elif self.path == "/" or self.path == "/index.html":
            self._send_html(200, self._welcome_page())
        else:
            self._send_json(404, {"error": "not found"})

    def _send_json(self, code: int, data: dict):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, code: int, html: str):
        body = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _welcome_page(self) -> str:
        status = _get_status_json()
        stock_count = status.get("stocks", "?")
        doc_count = status.get("documents", "?")
        nb_count = status.get("notebooks", "?")
        runs = status.get("recent_runs", [])

        runs_html = ""
        for r in runs[:5]:
            color = "#22c55e" if r["status"] == "success" else "#ef4444" if r["status"] == "failed" else "#eab308"
            runs_html += f"""<tr>
                <td>{r['phase']}</td>
                <td style="color:{color}">{r['status']}</td>
                <td>{r['rows_inserted'] or 0}</td>
                <td>{r['started_at'] or '-'}</td>
            </tr>"""

        return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>A股投研采集系统</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       max-width: 800px; margin: 40px auto; padding: 0 20px; background: #0f172a; color: #e2e8f0; }}
h1 {{ color: #38bdf8; }}
.card {{ background: #1e293b; border-radius: 12px; padding: 24px; margin: 16px 0; }}
.stats {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }}
.stat {{ text-align: center; }}
.stat-num {{ font-size: 32px; font-weight: bold; color: #38bdf8; }}
.stat-label {{ font-size: 14px; color: #94a3b8; margin-top: 4px; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #334155; font-size: 14px; }}
th {{ color: #94a3b8; font-weight: normal; }}
a {{ color: #38bdf8; }}
</style>
</head>
<body>
<h1>📈 A股投研资料采集系统</h1>
<p>服务正常运行中 · <a href="/status">JSON 状态</a></p>

<div class="card">
    <div class="stats">
        <div class="stat">
            <div class="stat-num">{stock_count}</div>
            <div class="stat-label">股票数量</div>
        </div>
        <div class="stat">
            <div class="stat-num">{doc_count}</div>
            <div class="stat-label">文档入口</div>
        </div>
        <div class="stat">
            <div class="stat-num">{nb_count}</div>
            <div class="stat-label">投研笔记本</div>
        </div>
    </div>
</div>

<div class="card">
    <h3>最近采集任务</h3>
    <table>
        <tr><th>任务</th><th>状态</th><th>新增行数</th><th>开始时间</th></tr>
        {runs_html or '<tr><td colspan="4" style="color:#94a3b8">暂无记录</td></tr>'}
    </table>
</div>
</body>
</html>"""

    def log_message(self, format, *args):
        # 静默日志，避免刷屏
        pass


def start_health_server(port: int = 8080) -> Thread:
    """在后台线程启动健康检查 HTTP 服务。"""
    def _run():
        try:
            server = HTTPServer(("0.0.0.0", port), HealthHandler)
            logger.info(f"健康检查服务启动在端口 {port}")
            server.serve_forever()
        except Exception as e:
            logger.warning(f"健康检查服务启动失败: {e}")

    t = Thread(target=_run, daemon=True, name="health-http")
    t.start()
    return t
