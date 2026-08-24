"""
CLI 入口：python -m china_stocks [command]

命令：
  init-db        初始化数据库（建 schema + 建表）
  phase-a        执行 Phase A（股票池构建）
  phase-daily    执行日线行情采集
  stock-basic    刷新 stock_basic 估值画像
  finance        刷新财务指标画像
  scheduler      启动调度器
  status         查看最近采集状态
"""

from __future__ import annotations

import argparse
import sys

from .config import db_url
from .logging_setup import logger


def cmd_init_db() -> None:
    """初始化数据库 schema 和表。

    逐条执行 SQL 语句，便于定位错误；失败的语句打印告警但不中断，
    保证尽可能多的表被创建。
    """
    from pathlib import Path

    sql_path = Path(__file__).resolve().parent.parent / "db" / "init.sql"
    if not sql_path.exists():
        logger.error(f"找不到初始化 SQL: {sql_path}")
        sys.exit(1)

    sql = sql_path.read_text(encoding="utf-8")
    from .db import get_engine
    import sqlalchemy

    schemas = ["sys", "raw", "src_akshare", "core", "biz"]
    engine = get_engine()

    # 1. 建 schema
    with engine.connect() as conn:
        for schema in schemas:
            conn.execute(sqlalchemy.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()

    # 2. 逐条执行 SQL 语句
    # 按分号切分，注意：DO $$ ... END $$; 块可能含分号，需要特殊处理
    statements = _split_sql_statements(sql)
    success = 0
    failed = 0
    with engine.connect() as conn:
        for i, stmt in enumerate(statements, 1):
            stmt = stmt.strip()
            if not stmt:
                continue
            try:
                conn.execute(sqlalchemy.text(stmt))
                conn.commit()
                success += 1
            except Exception as e:
                # 失败时回滚当前事务，继续下一条
                conn.rollback()
                failed += 1
                # 截取语句前 80 字方便识别
                preview = stmt.replace("\n", " ")[:80]
                logger.warning(
                    f"SQL 语句 #{i} 执行失败（已跳过）: {preview}...  错误: {e}"
                )

    logger.info(f"数据库初始化完成: {db_url()}  (成功 {success} 条, 失败 {failed} 条)")
    if failed > 0:
        logger.warning(f"有 {failed} 条 SQL 执行失败，请检查上方日志")


def _split_sql_statements(sql: str) -> list[str]:
    """
    将 SQL 文件拆分为单条语句。
    正确处理 DO $$ ... END $$ 这类包含分号的代码块。
    """
    statements: list[str] = []
    lines = sql.splitlines()
    current: list[str] = []
    in_do_block = False
    do_block_depth = 0

    for line in lines:
        stripped = line.strip()

        # 跳过空行和纯注释行
        if not stripped or stripped.startswith("--"):
            current.append(line)
            continue

        # 检测 DO $$ 开始
        if stripped.startswith("DO $$") or stripped == "DO $$":
            in_do_block = True
            do_block_depth = 1
            current.append(line)
            continue

        if in_do_block:
            current.append(line)
            # 检测 $$ 结束（简单处理：以 $$; 或 END $$; 结尾）
            if stripped.endswith("$$;") or stripped.endswith("END $$;"):
                do_block_depth -= 1
                if do_block_depth <= 0:
                    statements.append("\n".join(current))
                    current = []
                    in_do_block = False
            continue

        current.append(line)

        # 普通语句：以分号结尾（且不在字符串中，这里简化处理）
        if stripped.endswith(";"):
            statements.append("\n".join(current))
            current = []

    # 剩余内容（可能没分号）
    remaining = "\n".join(current).strip()
    if remaining:
        statements.append(remaining)

    return statements


def cmd_phase_a() -> None:
    from .src.phase_a_stock_pool import run_phase_a

    run_phase_a()


def cmd_phase_daily(args) -> None:
    from .src.phase_b_daily import run_phase_daily, fetch_daily
    from .src.phase_a_stock_pool import get_stock_codes

    if args.limit or args.codes:
        codes = args.codes.split(",") if args.codes else None
        if args.limit:
            if not codes:
                codes = get_stock_codes(limit=args.limit)
            else:
                codes = codes[: args.limit]
        fetch_daily(stock_codes=codes, incremental=not args.full)
    else:
        run_phase_daily()


def cmd_stock_basic() -> None:
    from .biz.stock_basic import run_stock_basic

    run_stock_basic()


def cmd_finance() -> None:
    from .biz.finance_snapshot import run_finance_snapshot

    run_finance_snapshot()


def cmd_capital() -> None:
    from .biz.capital_snapshot import run_capital_snapshot

    run_capital_snapshot()


def cmd_announcements(args) -> None:
    from .src.phase_b2_announcements import run_phase_b2_announcements

    run_phase_b2_announcements(
        incremental=not args.full,
        start_date=args.start,
        end_date=args.end,
    )


def cmd_download_announcements(args) -> None:
    from .src.phase_b2_download import run_download_announcements

    codes = args.codes.split(",") if args.codes else None
    dtypes = args.types.split(",") if args.types else None
    run_download_announcements(
        stock_codes=codes,
        limit=args.limit,
        doc_types=dtypes,
    )


def cmd_research(args) -> None:
    from .src.phase_b3_research import run_phase_b3_research

    run_phase_b3_research(
        incremental=not args.full,
        start_date=args.start,
        end_date=args.end,
    )


def cmd_survey(args) -> None:
    from .src.phase_b3_survey import run_phase_b3_survey

    run_phase_b3_survey(
        incremental=not args.full,
        start_date=args.start,
        end_date=args.end,
    )


def cmd_download_docs(args) -> None:
    from .src.phase_b2_download import download_docs
    from .sys import finish_run, start_run

    codes = args.codes.split(",") if args.codes else None
    dtypes = args.types.split(",") if args.types else None
    plats = args.platforms.split(",") if args.platforms else None

    run = start_run(
        platform_code="multi",
        phase="phase_b_download",
        target=f"limit={args.limit}, types={dtypes}, platforms={plats}",
    )
    try:
        success, failed = download_docs(
            stock_codes=codes,
            limit=args.limit,
            doc_types=dtypes,
            source_platforms=plats,
        )
        finish_run(run, status="success", rows_inserted=success, rows_updated=failed)
    except Exception as e:
        from .logging_setup import logger

        logger.exception(f"文档下载失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


def cmd_shareholder() -> None:
    from .biz.shareholder_snapshot import run_shareholder_snapshot

    run_shareholder_snapshot()


def cmd_notebook_build(args) -> None:
    from .biz.research_notebook import run_build_notebooks, build_notebook_for_stock

    if args.code:
        result = build_notebook_for_stock(args.code.zfill(6))
        print(f"已刷新: {result['stock_name']}（{result['stock_code']}）")
        print(f"完整度得分: {result['completeness_score']}/100")
        print(f"文档: {result['total_docs']} 条 (已下载 {result['downloaded_docs']})")
        print(f"事件: {result['total_events']} 条")
    else:
        run_build_notebooks(limit=args.limit or 0)


def cmd_notebook_info(args) -> None:
    from .biz.research_notebook import get_notebook_summary

    code = args.code.zfill(6)
    nb = get_notebook_summary(code)
    if not nb:
        print(f"未找到 {code} 的笔记本，请先运行 notebook-build")
        return

    print(f"\n{'=' * 60}")
    print(f"  {nb['stock_name']}（{nb['stock_code']}）")
    print(f"  行业: {nb['industry_l1']} / {nb['industry_l2']}")
    print(f"  完整度: {nb['completeness_score']}/100")
    print(f"  文档总数: {nb['total_docs']} (已下载 {nb['downloaded_docs']})")
    print(f"  事件总数: {nb['total_events']}")
    print(f"  最新财报期: {nb['latest_report_date']}")
    if nb["rating"]:
        print(f"  自评级: {nb['rating']}")
    print(f"{'=' * 60}")

    # 分类展示完整性
    c = nb.get("completeness", {})
    categories = {}
    for key, item in c.items():
        cat = item.get("category", "其他")
        categories.setdefault(cat, []).append(item)

    for cat, items in sorted(categories.items()):
        done = sum(1 for i in items if i["status"] == "done")
        print(f"\n【{cat}】({done}/{len(items)})")
        for item in sorted(items, key=lambda x: x["status"]):
            icon = (
                "✅"
                if item["status"] == "done"
                else "🟡"
                if item["status"] == "partial"
                else "❌"
            )
            print(
                f"  {icon} {item['label']}: {item['count']} (阈值 {item['threshold']})"
            )

    print()


def cmd_notebook_missing(args) -> None:
    from .biz.research_notebook import list_missing_items

    code = args.code.zfill(6)
    items = list_missing_items(code)
    if not items:
        print("资料齐全，没有缺失项 ✅")
        return

    print(f"\n缺失/不足的资料项 ({len(items)} 项):")
    for i, item in enumerate(items, 1):
        print(
            f"  {i}. [{item['category']}] {item['label']} ({item['key']}) — "
            f"当前 {item['count']}, 阈值 {item['threshold']}, 状态: {item['status']}"
        )
    print()


def cmd_notebook_fill(args) -> None:
    from .biz.notebook_fill import run_fill_notebook

    items = args.items.split(",") if args.items else None
    run_fill_notebook(args.code.zfill(6), items=items)


def cmd_parse_docs(args) -> None:
    from .biz.doc_parser import parse_docs, get_chunk_stats
    from .sys import finish_run, start_run

    codes = args.codes.split(",") if args.codes else None
    dtypes = args.types.split(",") if args.types else None

    if args.stats:
        stats = get_chunk_stats()
        print(f"总块数: {stats['total_chunks']}")
        print(f"总文档: {stats['total_docs']}")
        print(f"总股票: {stats['total_stocks']}")
        print(f"总 tokens: {stats['total_tokens']}")
        return

    run = start_run(
        platform_code="local",
        phase="phase_b_parse",
        target=f"limit={args.limit}, types={dtypes}",
    )
    try:
        docs, chunks = parse_docs(
            stock_codes=codes,
            limit=args.limit,
            doc_types=dtypes,
        )
        finish_run(run, status="success", rows_inserted=docs, rows_updated=chunks)
        print(f"解析完成: {docs} 篇文档, {chunks} 个块")
    except Exception as e:
        logger.exception(f"文档解析失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


def cmd_seed_demo(_args) -> None:
    from .seed_demo import run_seed_demo

    run_seed_demo()


def cmd_web(_args) -> None:
    """独立启动 Web 工作台（无调度器）。"""
    import time
    from .web_app import start_web_server

    start_web_server(port=8080)
    print("Web 工作台已启动: http://localhost:8080")
    print("按 Ctrl+C 退出")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n已退出")


def cmd_ask(args) -> None:
    from .biz.rag import ask_stock

    code = args.code.zfill(6)
    result = ask_stock(code, args.question, save_to_history=not args.no_save)

    print(f"\n{'=' * 60}")
    print(f"  Q: {args.question}")
    print(f"  股票: {code}  |  模型: {result['model']}")
    print(f"{'=' * 60}\n")
    print(result["answer"])

    if result["sources"]:
        print(f"\n引用来源 ({len(result['sources'])}):")
        for i, s in enumerate(result["sources"], 1):
            print(f"  [{i}] {s['title']}")
            print(f"      {s['doc_type']} · {s['publish_date']} · {s['source']}")
            if s.get("url"):
                print(f"      {s['url']}")
    print()


def cmd_events() -> None:
    from .src.phase_d_events import run_corporate_events

    run_corporate_events()


def cmd_scheduler() -> None:
    from .scheduler import start_scheduler

    start_scheduler()


def cmd_status(_args) -> None:
    """查看最近 10 条采集记录。"""
    from sqlalchemy import text
    from .db import get_session

    with get_session() as sess:
        rows = sess.execute(
            text("""
            SELECT run_id, phase, status, rows_inserted, rows_updated,
                   started_at, cost_seconds, error_msg
            FROM sys.ingest_run
            ORDER BY run_id DESC
            LIMIT 10
        """)
        ).fetchall()

    if not rows:
        print("暂无采集记录")
        return

    print(
        f"{'ID':<6}{'Phase':<24}{'Status':<10}{'Ins':>6}{'Upd':>6}  "
        f"{'Started At':<22}{'Cost(s)':>8}"
    )
    print("-" * 90)
    for r in rows:
        cost = f"{r.cost_seconds:.1f}" if r.cost_seconds else "-"
        started = r.started_at.strftime("%Y-%m-%d %H:%M:%S") if r.started_at else "-"
        print(
            f"{r.run_id:<6}{r.phase:<24}{r.status:<10}{r.rows_inserted or 0:>6}"
            f"{r.rows_updated or 0:>6}  {started:<22}{cost:>8}"
        )
        if r.error_msg:
            print(f"       error: {r.error_msg[:100]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="china_stocks", description="A 股投研资料采集系统"
    )
    sub = parser.add_subparsers(dest="command", help="可用命令")

    sub.add_parser("init-db", help="初始化数据库")
    sub.add_parser("phase-a", help="Phase A: 股票池构建")

    p_daily = sub.add_parser("phase-daily", help="日线行情采集")
    p_daily.add_argument("--codes", type=str, help="指定股票代码，逗号分隔")
    p_daily.add_argument("--limit", type=int, help="限制数量（调试用）")
    p_daily.add_argument("--full", action="store_true", help="全量重拉（默认增量）")

    sub.add_parser("stock-basic", help="刷新估值画像")
    sub.add_parser("finance", help="刷新财务指标")
    sub.add_parser("capital", help="刷新资金面画像（北向+融资融券）")
    sub.add_parser("shareholder", help="刷新股东画像（十大股东+质押+户数）")
    sub.add_parser("events", help="采集公司事件（分红/解禁/业绩预告/回购/增减持）")

    p_ann = sub.add_parser("announcements", help="公告入口采集（巨潮资讯网）")
    p_ann.add_argument("--start", type=str, help="起始日期 YYYYMMDD")
    p_ann.add_argument("--end", type=str, help="结束日期 YYYYMMDD")
    p_ann.add_argument("--full", action="store_true", help="非增量（从指定日期开始）")

    p_research = sub.add_parser("research", help="券商研报入口采集（东财研报中心）")
    p_research.add_argument("--start", type=str, help="起始日期 YYYYMMDD")
    p_research.add_argument("--end", type=str, help="结束日期 YYYYMMDD")
    p_research.add_argument(
        "--full", action="store_true", help="非增量（从指定日期开始）"
    )

    p_survey = sub.add_parser("survey", help="机构调研纪要采集（东财）")
    p_survey.add_argument("--start", type=str, help="起始日期 YYYYMMDD")
    p_survey.add_argument("--end", type=str, help="结束日期 YYYYMMDD")
    p_survey.add_argument(
        "--full", action="store_true", help="非增量（从指定日期开始）"
    )

    p_dl = sub.add_parser("download-docs", help="下载文档 PDF 到本地（公告/研报/调研）")
    p_dl.add_argument("--codes", type=str, help="指定股票代码，逗号分隔")
    p_dl.add_argument("--limit", type=int, help="限制下载数量")
    p_dl.add_argument(
        "--types",
        type=str,
        help="指定文档类型（announcement/research/survey...），逗号分隔",
    )
    p_dl.add_argument(
        "--platforms",
        type=str,
        help="指定来源平台（cninfo/eastmoney_research...），逗号分隔",
    )

    # 兼容旧命令
    p_dl_old = sub.add_parser("download-ann", help="（兼容）下载公告 PDF 到本地")
    p_dl_old.add_argument("--codes", type=str, help="指定股票代码，逗号分隔")
    p_dl_old.add_argument("--limit", type=int, help="限制下载数量")
    p_dl_old.add_argument("--types", type=str, help="指定公告类型，逗号分隔")

    sub.add_parser("scheduler", help="启动调度器")
    sub.add_parser("status", help="查看采集状态")

    # ── Phase D: 投研笔记本 ──
    nb = sub.add_parser("notebook-build", help="构建/刷新投研笔记本（完整性清单）")
    nb.add_argument("--limit", type=int, help="限制数量（调试用）")
    nb.add_argument("--code", type=str, help="单只股票代码")

    nb_info = sub.add_parser("notebook-info", help="查看某只股票的笔记本概览")
    nb_info.add_argument("code", type=str, help="股票代码")

    nb_missing = sub.add_parser("notebook-missing", help="列出缺失资料")
    nb_missing.add_argument("code", type=str, help="股票代码")

    nb_fill = sub.add_parser("notebook-fill", help="一键补齐缺失资料")
    nb_fill.add_argument("code", type=str, help="股票代码")
    nb_fill.add_argument("--items", type=str, help="指定补齐项，逗号分隔")

    ask = sub.add_parser("ask", help="对某只股票提问（RAG 问答）")
    ask.add_argument("code", type=str, help="股票代码")
    ask.add_argument("question", type=str, help="问题")
    ask.add_argument("--no-save", action="store_true", help="不保存对话历史")

    p_parse = sub.add_parser(
        "parse-docs", help="解析已下载的文档 PDF，切块写入 doc_chunk"
    )
    p_parse.add_argument("--codes", type=str, help="指定股票代码，逗号分隔")
    p_parse.add_argument("--limit", type=int, help="限制解析数量")
    p_parse.add_argument(
        "--types",
        type=str,
        help="指定文档类型（announcement/research/survey...），逗号分隔",
    )
    p_parse.add_argument("--stats", action="store_true", help="查看切块统计信息")

    sub.add_parser("seed-demo", help="填充 demo 示例数据（本地演示/调试用）")
    sub.add_parser("web", help="启动 Web 工作台（独立运行，无调度器）")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "init-db": lambda _: cmd_init_db(),
        "phase-a": lambda _: cmd_phase_a(),
        "phase-daily": cmd_phase_daily,
        "stock-basic": lambda _: cmd_stock_basic(),
        "finance": lambda _: cmd_finance(),
        "capital": lambda _: cmd_capital(),
        "shareholder": lambda _: cmd_shareholder(),
        "events": lambda _: cmd_events(),
        "announcements": cmd_announcements,
        "research": cmd_research,
        "survey": cmd_survey,
        "download-docs": cmd_download_docs,
        "download-ann": cmd_download_announcements,
        "scheduler": lambda _: cmd_scheduler(),
        "status": cmd_status,
        "notebook-build": cmd_notebook_build,
        "notebook-info": cmd_notebook_info,
        "notebook-missing": cmd_notebook_missing,
        "notebook-fill": cmd_notebook_fill,
        "ask": cmd_ask,
        "parse-docs": cmd_parse_docs,
        "seed-demo": cmd_seed_demo,
        "web": cmd_web,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
