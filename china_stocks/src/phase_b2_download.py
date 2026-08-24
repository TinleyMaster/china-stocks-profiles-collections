"""
文档 PDF 通用下载器（公告 / 研报 / 调研纪要）。

设计原则（贴合你的文件处理偏好）：
  1. 先全部下载到本地，收集完信息后再上传/处理
  2. 保留文件原始名称，方便研报编写时查找
  3. 目录结构：{DATA_DIR}/{stock_code}/{doc_type}/{YYYY}/{filename}
     doc_type: announcements / research / survey / prospectus ...

下载来源：
  - 巨潮资讯网公告 PDF（source_platform = cninfo）
  - 东方财富研报 PDF（source_platform = eastmoney_research）
  - 支持断点续下：is_downloaded 标记

不同来源的 Referer 和 headers 不同，按 source_platform 自动适配。
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import text
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from ..config import MAX_WORKERS, ROOT_DIR
from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run


# 数据根目录
DATA_DIR = ROOT_DIR / "data" / "docs"

# 不同来源的 HTTP 头
HEADERS_MAP = {
    "cninfo": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf,*/*",
        "Referer": "http://www.cninfo.com.cn/",
    },
    "eastmoney_research": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf,*/*",
        "Referer": "https://data.eastmoney.com/",
    },
    "eastmoney": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf,*/*",
        "Referer": "https://www.eastmoney.com/",
    },
}

DEFAULT_HEADERS = HEADERS_MAP["cninfo"]

# doc_type → 子目录映射
DOCTYPE_DIR_MAP = {
    "announcement": "announcements",
    "annual_report": "annual_reports",
    "semi_annual_report": "semi_annual_reports",
    "quarterly_report": "quarterly_reports",
    "research": "research_reports",
    "survey": "survey_notes",
    "prospectus": "prospectus",
    "listing": "listing",
    "other": "other",
}


def _get_download_dir(stock_code: str, doc_type: str, year: int) -> Path:
    """获取本地存储目录：{DATA_DIR}/{stock_code}/{doc_dir}/{YYYY}/"""
    doc_dir = DOCTYPE_DIR_MAP.get(doc_type, doc_type)
    p = DATA_DIR / stock_code / doc_dir / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sanitize_filename(name: str) -> str:
    """清洗文件名，移除非法字符。"""
    # 保留中文、字母、数字、常见符号
    cleaned = re.sub(r"[\\/:*?\"<>|\r\n\t]", "_", name)
    cleaned = cleaned.strip().strip(".")
    if len(cleaned) > 200:
        cleaned = cleaned[:196] + ".pdf"
    if not cleaned.lower().endswith(".pdf"):
        cleaned += ".pdf"
    return cleaned


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
    reraise=True,
)
def _download_pdf(url: str, save_path: Path, source_platform: str = "cninfo") -> int:
    """下载单个 PDF 文件，返回文件大小（字节）。"""
    headers = HEADERS_MAP.get(source_platform, DEFAULT_HEADERS)
    resp = requests.get(url, headers=headers, timeout=30, stream=True)
    resp.raise_for_status()

    total = 0
    with open(save_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                total += len(chunk)
    return total


def get_pending_downloads(
    stock_codes: Optional[list[str]] = None,
    limit: int = 0,
    doc_types: Optional[list[str]] = None,
    source_platforms: Optional[list[str]] = None,
) -> list[dict]:
    """
    获取待下载的文档列表。

    Args:
        stock_codes: 指定股票代码，None = 全部
        limit: 限制数量
        doc_types: 指定文档类型，None = 全部
        source_platforms: 指定来源平台，None = 全部
    """
    sql = """
        SELECT id, stock_code, title, publish_date, url, doc_type, source_platform
        FROM biz.doc_source_entry
        WHERE is_downloaded = FALSE
          AND url IS NOT NULL
          AND url != ''
    """
    params = {}

    if stock_codes:
        sql += " AND stock_code = ANY(:codes)"
        params["codes"] = stock_codes

    if doc_types:
        sql += " AND doc_type = ANY(:dtypes)"
        params["dtypes"] = doc_types

    if source_platforms:
        sql += " AND source_platform = ANY(:plats)"
        params["plats"] = source_platforms

    sql += " ORDER BY publish_date DESC"

    if limit and limit > 0:
        sql += " LIMIT :lim"
        params["lim"] = limit

    with get_session() as sess:
        rows = sess.execute(text(sql), params).fetchall()

    return [
        {
            "id": r.id,
            "stock_code": r.stock_code,
            "title": r.title,
            "publish_date": r.publish_date,
            "url": r.url,
            "doc_type": r.doc_type,
            "source_platform": r.source_platform,
        }
        for r in rows
    ]


def _resolve_pdf_url(url: str, source_platform: str, pub_date) -> str:
    """将详情页 URL 转换为真实 PDF 下载链接。

    巨潮资讯的公告链接通常是详情页 HTML，需要转换为 static.cninfo.com.cn 的 PDF 直链。
    """
    if source_platform == "cninfo" and "cninfo.com.cn" in url:
        # 尝试从 URL 中提取 announcementId 和 announcementTime
        import re as _re
        m_id = _re.search(r"announcementId=([^&]+)", url)
        m_time = _re.search(r"announcementTime=([^&]+)", url)
        if m_id and m_time:
            ann_id = m_id.group(1)
            ann_time = m_time.group(1)
            # 巨潮 PDF 直链格式
            return f"https://static.cninfo.com.cn/finalpage/{ann_time}/{ann_id}.PDF"
        # 如果已经是 static 域名的 PDF，直接返回
        if "static.cninfo.com.cn" in url and url.lower().endswith(".pdf"):
            return url
    return url


def _download_one(item: dict) -> tuple[int, bool, str]:
    """
    下载单条文档。
    返回 (entry_id, 是否成功, 本地路径或错误信息)
    """
    eid = item["id"]
    url = item["url"]
    stock_code = item["stock_code"]
    title = item["title"]
    pub_date = item["publish_date"]
    doc_type = item.get("doc_type", "announcement")
    source_platform = item.get("source_platform", "cninfo")

    try:
        # 将详情页 URL 解析为真实 PDF 链接
        url = _resolve_pdf_url(url, source_platform, pub_date)

        year = pub_date.year if pub_date else 2000
        filename = _sanitize_filename(f"{pub_date}_{title}" if pub_date else title)
        save_path = _get_download_dir(stock_code, doc_type, year) / filename

        # 如果文件已存在且大小正常，跳过下载直接标记
        if save_path.exists() and save_path.stat().st_size > 1000:
            return eid, True, str(save_path)

        size = _download_pdf(url, save_path, source_platform)

        if size < 1000:
            return eid, False, f"文件过小: {size} bytes"

        return eid, True, str(save_path)

    except Exception as e:
        return eid, False, str(e)


def download_announcements(
    stock_codes: Optional[list[str]] = None,
    limit: int = 0,
    doc_types: Optional[list[str]] = None,
) -> tuple[int, int]:
    """批量下载公告 PDF（向后兼容，等价于 download_docs）。"""
    return download_docs(stock_codes=stock_codes, limit=limit, doc_types=doc_types)


def download_docs(
    stock_codes: Optional[list[str]] = None,
    limit: int = 0,
    doc_types: Optional[list[str]] = None,
    source_platforms: Optional[list[str]] = None,
) -> tuple[int, int]:
    """
    批量下载文档 PDF（公告 / 研报 / 调研纪要）。
    返回 (成功数, 失败数)
    """
    items = get_pending_downloads(
        stock_codes=stock_codes,
        limit=limit,
        doc_types=doc_types,
        source_platforms=source_platforms,
    )
    if not items:
        logger.info("没有待下载的文档")
        return 0, 0

    logger.info(f"待下载文档: {len(items)} 条")

    success = 0
    failed = 0
    success_items: list[tuple[int, str, int]] = []  # (id, file_path, index)

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 4)) as pool:
        futures = {pool.submit(_download_one, item): item for item in items}
        for i, fut in enumerate(as_completed(futures), 1):
            eid, ok, result = fut.result()
            if ok:
                success += 1
                success_items.append((eid, result, i))
            else:
                failed += 1
                logger.debug(f"下载失败 #{eid}: {result}")

            if i % 100 == 0:
                logger.info(
                    f"下载进度: {i}/{len(items)}, 成功 {success}, 失败 {failed}"
                )
                # 每 100 条批量更新一次数据库
                _mark_downloaded(
                    success_items[-100:] if len(success_items) >= 100 else success_items
                )
                # 限速
                time.sleep(1)

    # 最后再刷一次
    if success_items:
        _mark_downloaded(success_items)

    logger.info(f"下载完成: 成功 {success}, 失败 {failed}")
    return success, failed


def _mark_downloaded(items: list[tuple[int, str, int]]) -> None:
    """批量标记已下载。"""
    if not items:
        return
    with get_session() as sess:
        for eid, file_path, _ in items:
            sess.execute(
                text("""
                UPDATE biz.doc_source_entry SET
                    is_downloaded = TRUE,
                    file_path = :fp,
                    file_size = :fs
                WHERE id = :id
            """),
                {
                    "id": eid,
                    "fp": file_path,
                    "fs": Path(file_path).stat().st_size
                    if Path(file_path).exists()
                    else None,
                },
            )


def run_download_announcements(
    stock_codes: Optional[list[str]] = None,
    limit: int = 0,
    doc_types: Optional[list[str]] = None,
) -> None:
    """执行公告下载任务。"""
    run = start_run(
        platform_code="cninfo",
        phase="phase_b2_download",
        target=f"limit={limit}, types={doc_types}",
    )
    try:
        # 检查上游依赖：biz.doc_source_entry 中 is_downloaded=FALSE 且 url 不为空的记录
        pending = get_pending_downloads(
            stock_codes=stock_codes, limit=limit, doc_types=doc_types
        )
        if not pending:
            finish_run(
                run,
                status="skipped",
                error_msg="biz.doc_source_entry 中无待下载文档（is_downloaded=FALSE 且 url 不为空）",
            )
            logger.warning("公告下载跳过：无待下载文档")
            return

        success, failed = download_announcements(
            stock_codes=stock_codes, limit=limit, doc_types=doc_types
        )

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=success,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=success, rows_updated=failed, error_msg=err_msg)
        if status != "success":
            logger.warning(f"公告下载结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"公告下载失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_download_announcements(limit=100)
