"""
文档解析与切块模块。

功能：
  1. PDF 文本提取（PyMuPDF，纯文本，不做 OCR）
  2. 智能文本切块（按段落边界 + token 限制 + 重叠窗口）
  3. 切块写入 biz.doc_chunk 表

设计原则（低成本优先）：
  - embedding 向量化是可选的，pgvector 没装也能用关键词检索
  - 切块策略偏向保守，宁可切块稍小也不要信息截断
  - token 计数用 tiktoken（cl100k_base），适配大多数 embedding 模型
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run

# 尝试导入可选依赖
try:
    import fitz  # PyMuPDF

    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

try:
    import tiktoken

    _tokenizer = tiktoken.get_encoding("cl100k_base")
    HAS_TIKTOKEN = True
except ImportError:
    _tokenizer = None
    HAS_TIKTOKEN = False


# 默认切块参数
DEFAULT_MAX_TOKENS = 500  # 单块最大 token 数
DEFAULT_OVERLAP_TOKENS = 50  # 重叠 token 数
DEFAULT_MIN_CHARS = 30  # 块最小字符数（过短的块丢弃）


@dataclass
class DocChunk:
    """文档块数据结构。"""

    doc_id: int
    stock_code: str
    chunk_index: int
    chunk_text: str
    chunk_tokens: int = 0


# ============================================================
# 1. PDF 文本提取
# ============================================================


def extract_pdf_text(pdf_path: str | Path) -> str:
    """
    从 PDF 文件提取纯文本。

    Args:
        pdf_path: PDF 文件路径

    Returns:
        提取的纯文本（按页用分隔符拼接）

    Raises:
        RuntimeError: PyMuPDF 未安装或提取失败
    """
    if not HAS_PYMUPDF:
        raise RuntimeError("PyMuPDF 未安装，请先 pip install pymupdf")

    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF 文件不存在: {path}")

    try:
        doc = fitz.open(path)
        pages = []
        for page_num, page in enumerate(doc, 1):
            text = page.get_text("text")
            if text.strip():
                pages.append(f"--- 第 {page_num} 页 ---\n{text.strip()}")
        doc.close()
        return "\n\n".join(pages)
    except Exception as e:
        raise RuntimeError(f"PDF 提取失败 {path}: {e}") from e


# ============================================================
# 2. 文本切块
# ============================================================


def count_tokens(text: str) -> int:
    """
    计算文本的 token 数。
    优先用 tiktoken，没有的话用字符数粗估（中文约 1.5 字 = 1 token，英文 4 字符 = 1 token）。
    """
    if HAS_TIKTOKEN and _tokenizer:
        try:
            return len(_tokenizer.encode(text))
        except Exception:
            pass
    # 粗略估计：中文每个字约 0.7 token，英文每个词约 0.75 token
    # 简化：字符数 / 2（混合语言的保守估计）
    return max(1, len(text) // 2)


def _split_by_paragraphs(text: str) -> list[str]:
    """
    按段落分割文本。
    段落判定：连续两个以上换行，或者有明显的段落标记。
    """
    # 先统一换行符
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # 移除分页标记
    text = re.sub(r"---\s*第\s*\d+\s*页\s*---\n?", "", text)

    # 按空行分割段落
    raw_paragraphs = re.split(r"\n\s*\n", text)

    paragraphs = []
    for p in raw_paragraphs:
        p = p.strip()
        if p and len(p) >= DEFAULT_MIN_CHARS:
            # 清理多余空白
            p = re.sub(r"[ \t]+", " ", p)
            p = re.sub(r"\n{2,}", "\n", p)
            paragraphs.append(p)

    return paragraphs


def _merge_paragraphs_to_chunks(
    paragraphs: list[str],
    max_tokens: int = DEFAULT_MAX_TOKENS,
    overlap_tokens: int = DEFAULT_OVERLAP_TOKENS,
) -> list[str]:
    """
    将段落合并成大小合适的块。

    策略：
      - 逐个段落累加，直到接近 max_tokens
      - 如果单个段落就超过 max_tokens，按句子拆分
      - 块之间保留 overlap_tokens 的重叠
    """
    chunks: list[str] = []
    current_chunk_parts: list[str] = []
    current_tokens = 0

    for para in paragraphs:
        para_tokens = count_tokens(para)

        # 单个段落超过最大 token，需要按句子拆分
        if para_tokens > max_tokens:
            # 先把当前累积的 flush 掉
            if current_chunk_parts:
                chunks.append("\n\n".join(current_chunk_parts))
                current_chunk_parts = []
                current_tokens = 0

            # 按句子拆分长段落
            sub_chunks = _split_long_paragraph(para, max_tokens, overlap_tokens)
            chunks.extend(sub_chunks)
            continue

        # 加上这个段落后超限，先 flush 当前块
        if current_tokens + para_tokens > max_tokens and current_chunk_parts:
            chunks.append("\n\n".join(current_chunk_parts))

            # 计算重叠：从当前块末尾取 overlap_tokens 的内容作为下一块开头
            overlap_text = _take_tokens_from_end(
                "\n\n".join(current_chunk_parts), overlap_tokens
            )
            if overlap_text:
                current_chunk_parts = [overlap_text, para]
                current_tokens = count_tokens(overlap_text) + para_tokens
            else:
                current_chunk_parts = [para]
                current_tokens = para_tokens
        else:
            current_chunk_parts.append(para)
            current_tokens += para_tokens

    # flush 最后一块
    if current_chunk_parts:
        chunks.append("\n\n".join(current_chunk_parts))

    return chunks


def _split_long_paragraph(
    text: str,
    max_tokens: int,
    overlap_tokens: int,
) -> list[str]:
    """
    对超长段落按句子拆分。
    句子分隔符：。！？.!? 以及换行。
    """
    # 按句子切分（保留分隔符）
    sentences = re.split(r"(?<=[。！？.!?])\s*", text)
    sentences = [s for s in sentences if s.strip()]

    chunks: list[str] = []
    current_parts: list[str] = []
    current_tokens = 0

    for sent in sentences:
        sent_tokens = count_tokens(sent)

        # 单句就超长（极端情况），直接按字符硬切
        if sent_tokens > max_tokens:
            if current_parts:
                chunks.append("".join(current_parts))
                current_parts = []
                current_tokens = 0

            # 硬切：按 max_tokens 切成小块
            char_per_token = max(1, len(sent) // sent_tokens)
            chunk_size = max_tokens * char_per_token
            for i in range(0, len(sent), chunk_size - overlap_tokens * char_per_token):
                chunk = sent[i : i + chunk_size]
                if len(chunk) >= DEFAULT_MIN_CHARS:
                    chunks.append(chunk)
            continue

        if current_tokens + sent_tokens > max_tokens and current_parts:
            chunks.append("".join(current_parts))

            # 重叠
            overlap_text = _take_tokens_from_end("".join(current_parts), overlap_tokens)
            if overlap_text:
                current_parts = [overlap_text, sent]
                current_tokens = count_tokens(overlap_text) + sent_tokens
            else:
                current_parts = [sent]
                current_tokens = sent_tokens
        else:
            current_parts.append(sent)
            current_tokens += sent_tokens

    if current_parts:
        chunks.append("".join(current_parts))

    return chunks


def _take_tokens_from_end(text: str, n_tokens: int) -> str:
    """从文本末尾取大约 n_tokens 的内容。"""
    if n_tokens <= 0:
        return ""

    total = count_tokens(text)
    if total <= n_tokens:
        return text

    # 估算需要截取的字符数（从末尾往前）
    char_per_token = max(1, len(text) // total)
    target_chars = n_tokens * char_per_token * 2  # 多取一些，保险

    candidate = text[-target_chars:] if len(text) > target_chars else text

    # 找到一个句子/段落边界，避免从中间切开
    # 从前往后找第一个句号、感叹号、问号后的位置
    match = re.search(r"[。！？.!?\n]", candidate)
    if match and match.start() > 0:
        candidate = candidate[match.start() + 1 :]

    # 还是可能太大，再 token 精确截断一下
    if HAS_TIKTOKEN and _tokenizer:
        try:
            tokens = _tokenizer.encode(candidate)
            if len(tokens) > n_tokens:
                tokens = tokens[-n_tokens:]
                candidate = _tokenizer.decode(tokens)
        except Exception:
            pass

    return candidate.strip()


def chunk_text(
    text: str,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    overlap_tokens: int = DEFAULT_OVERLAP_TOKENS,
) -> list[str]:
    """
    对文本进行智能切块。

    Args:
        text: 原始文本
        max_tokens: 单块最大 token 数
        overlap_tokens: 块之间重叠的 token 数

    Returns:
        切块后的文本列表
    """
    if not text or not text.strip():
        return []

    # 1. 按段落分割
    paragraphs = _split_by_paragraphs(text)
    if not paragraphs:
        return []

    # 2. 合并段落成块
    chunks = _merge_paragraphs_to_chunks(paragraphs, max_tokens, overlap_tokens)

    # 3. 过滤过短的块
    chunks = [c for c in chunks if len(c.strip()) >= DEFAULT_MIN_CHARS]

    return chunks


# ============================================================
# 3. 切块写入数据库
# ============================================================


def get_pending_parse_docs(
    stock_codes: Optional[list[str]] = None,
    limit: int = 0,
    doc_types: Optional[list[str]] = None,
) -> list[dict]:
    """
    获取待解析的文档（已下载但未切块的）。

    判定：is_downloaded = TRUE 且 doc_chunk 表中没有对应记录
    """
    sql = """
        SELECT d.id, d.stock_code, d.title, d.publish_date, d.file_path,
               d.doc_type, d.source_platform
        FROM biz.doc_source_entry d
        WHERE d.is_downloaded = TRUE
          AND d.file_path IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM biz.doc_chunk c WHERE c.doc_id = d.id
          )
    """
    params: dict = {}

    if stock_codes:
        sql += " AND d.stock_code = ANY(:codes)"
        params["codes"] = stock_codes

    if doc_types:
        sql += " AND d.doc_type = ANY(:dtypes)"
        params["dtypes"] = doc_types

    sql += " ORDER BY d.publish_date DESC"

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
            "file_path": r.file_path,
            "doc_type": r.doc_type,
            "source_platform": r.source_platform,
        }
        for r in rows
    ]


def parse_and_chunk_doc(doc_info: dict) -> tuple[int, int]:
    """
    解析单个文档并写入切块表。

    Args:
        doc_info: 文档信息字典，至少包含 id, stock_code, file_path, title

    Returns:
        (doc_id, chunk_count)
    """
    doc_id = doc_info["id"]
    stock_code = doc_info["stock_code"]
    file_path = doc_info["file_path"]
    title = doc_info.get("title", "")

    # 检查文件是否存在
    if not Path(file_path).exists():
        logger.warning(f"文件不存在，跳过解析: doc_id={doc_id}, path={file_path}")
        return doc_id, 0

    try:
        # 1. 提取文本
        full_text = extract_pdf_text(file_path)
        if not full_text.strip():
            logger.warning(f"PDF 无文本内容（可能是扫描件）: doc_id={doc_id}")
            return doc_id, 0

        # 2. 切块
        chunks = chunk_text(full_text)
        if not chunks:
            logger.warning(f"切块结果为空: doc_id={doc_id}")
            return doc_id, 0

        # 3. 写入数据库
        with get_session() as sess:
            # 先清掉旧的切块（幂等）
            sess.execute(
                text("DELETE FROM biz.doc_chunk WHERE doc_id = :did"),
                {"did": doc_id},
            )

            for idx, chunk_text_str in enumerate(chunks):
                n_tokens = count_tokens(chunk_text_str)
                sess.execute(
                    text("""
                    INSERT INTO biz.doc_chunk
                        (doc_id, stock_code, chunk_index, chunk_text, chunk_tokens)
                    VALUES (:did, :code, :idx, :txt, :toks)
                """),
                    {
                        "did": doc_id,
                        "code": stock_code,
                        "idx": idx,
                        "txt": chunk_text_str,
                        "toks": n_tokens,
                    },
                )

        logger.info(
            f"文档解析完成: doc_id={doc_id}, {len(chunks)} 块, "
            f"约 {sum(count_tokens(c) for c in chunks)} tokens, 标题: {title[:50]}"
        )
        return doc_id, len(chunks)

    except Exception as e:
        logger.error(f"文档解析失败 doc_id={doc_id}: {e}")
        return doc_id, 0


def parse_docs(
    stock_codes: Optional[list[str]] = None,
    limit: int = 0,
    doc_types: Optional[list[str]] = None,
) -> tuple[int, int]:
    """
    批量解析已下载的文档。

    Args:
        stock_codes: 指定股票代码
        limit: 限制数量
        doc_types: 指定文档类型

    Returns:
        (成功文档数, 总块数)
    """
    docs = get_pending_parse_docs(
        stock_codes=stock_codes,
        limit=limit,
        doc_types=doc_types,
    )

    if not docs:
        logger.info("没有待解析的文档")
        return 0, 0

    logger.info(f"待解析文档: {len(docs)} 条")

    success_docs = 0
    total_chunks = 0

    for i, doc in enumerate(docs, 1):
        doc_id, n_chunks = parse_and_chunk_doc(doc)
        if n_chunks > 0:
            success_docs += 1
            total_chunks += n_chunks

        if i % 20 == 0:
            logger.info(
                f"解析进度: {i}/{len(docs)}, 成功 {success_docs} 篇, "
                f"总块数 {total_chunks}"
            )

    logger.info(f"解析完成: {success_docs}/{len(docs)} 篇文档, 共 {total_chunks} 个块")
    return success_docs, total_chunks


# ============================================================
# 3.5 向量化（可选，pgvector + embedding 可用时启用）
# ============================================================

def _pgvector_available() -> bool:
    """检查 pgvector 扩展是否已安装。"""
    try:
        with get_session() as sess:
            row = sess.execute(text(
                "SELECT 1 FROM pg_extension WHERE extname = 'vector'"
            )).fetchone()
            return row is not None
    except Exception as e:
        logger.warning(f"检查 pgvector 可用性失败: {e}")
        return False


def embed_pending_chunks(batch_size: int = 100) -> int:
    """
    为 doc_chunk 中尚无 embedding 的块生成向量（可选功能）。

    前提：pgvector 已安装 + LLM embedding 可用。
    不满足时直接返回 0，不报错。

    返回更新的块数。
    """
    if not _pgvector_available():
        logger.info("pgvector 未安装，跳过向量化")
        return 0

    try:
        from .llm_client import embed as llm_embed, is_available as llm_available
    except ImportError:
        logger.info("LLM 客户端不可用，跳过向量化")
        return 0

    if not llm_available():
        logger.info("LLM embedding 不可用，跳过向量化")
        return 0

    updated = 0
    while True:
        with get_session() as sess:
            rows = sess.execute(text("""
                SELECT id, chunk_text FROM biz.doc_chunk
                WHERE embedding IS NULL
                ORDER BY id
                LIMIT :bs
            """), {"bs": batch_size}).fetchall()

        if not rows:
            break

        chunk_ids = [r.id for r in rows]
        texts = [r.chunk_text for r in rows]

        try:
            embeddings = llm_embed(texts)
        except Exception as e:
            logger.warning(f"embedding 调用失败: {e}")
            break

        if not embeddings or len(embeddings) != len(chunk_ids):
            logger.warning(f"embedding 返回数量不匹配: {len(embeddings)} vs {len(chunk_ids)}")
            break

        # 批量更新
        with get_session() as sess:
            for cid, emb in zip(chunk_ids, embeddings):
                if not emb:
                    continue
                vec_str = "[" + ",".join(str(v) for v in emb) + "]"
                sess.execute(text("""
                    UPDATE biz.doc_chunk
                    SET embedding = :vec::vector
                    WHERE id = :cid
                """), {"cid": cid, "vec": vec_str})

        updated += len(chunk_ids)
        logger.info(f"向量化进度: {updated} 块已更新")

    if updated > 0:
        logger.info(f"向量化完成: 共更新 {updated} 个块")
    return updated


# ============================================================
# 4. 统计信息
# ============================================================


def get_chunk_stats(stock_code: Optional[str] = None) -> dict:
    """获取切块统计信息。"""
    sql = """
        SELECT COUNT(*) as total_chunks,
               COUNT(DISTINCT doc_id) as total_docs,
               COUNT(DISTINCT stock_code) as total_stocks,
               SUM(chunk_tokens) as total_tokens
        FROM biz.doc_chunk
    """
    params = {}
    if stock_code:
        sql += " WHERE stock_code = :code"
        params["code"] = stock_code

    with get_session() as sess:
        row = sess.execute(text(sql), params).fetchone()

    return {
        "total_chunks": row.total_chunks or 0,
        "total_docs": row.total_docs or 0,
        "total_stocks": row.total_stocks or 0,
        "total_tokens": row.total_tokens or 0,
    }


def run_parse_docs(
    stock_codes: Optional[list[str]] = None,
    limit: int = 0,
    doc_types: Optional[list[str]] = None,
) -> None:
    """执行文档解析任务（带 ingest_run 记录）。"""
    run = start_run(
        platform_code="local",
        phase="phase_b_parse",
        target=f"limit={limit}, types={doc_types}",
    )
    try:
        # 检查上游依赖：biz.doc_source_entry 中 is_downloaded=TRUE 的记录
        with get_session() as sess:
            downloaded_count = sess.execute(text("""
                SELECT COUNT(*) FROM biz.doc_source_entry
                WHERE is_downloaded = TRUE AND file_path IS NOT NULL
            """)).fetchone()[0]
        if downloaded_count == 0:
            finish_run(
                run,
                status="skipped",
                error_msg="biz.doc_source_entry 中无已下载文档（is_downloaded=TRUE）",
            )
            logger.warning("文档解析跳过：无已下载文档")
            return

        docs, chunks = parse_docs(
            stock_codes=stock_codes,
            limit=limit,
            doc_types=doc_types,
        )

        # 可选：为新切块生成向量 embedding（pgvector + LLM 可用时执行）
        embed_count = 0
        try:
            embed_count = embed_pending_chunks(batch_size=100)
            if embed_count > 0:
                logger.info(f"向量 embedding 生成: {embed_count} 块")
        except Exception as e:
            logger.warning(f"向量 embedding 生成失败（不影响主流程）: {e}")

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=docs,
            rows_updated=chunks,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=docs, rows_updated=chunks + embed_count, error_msg=err_msg)
        if status != "success":
            logger.warning(f"文档解析结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"文档解析失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    # 简单自测：统计当前切块情况
    stats = get_chunk_stats()
    print(f"总块数: {stats['total_chunks']}")
    print(f"总文档: {stats['total_docs']}")
    print(f"总股票: {stats['total_stocks']}")
    print(f"总 tokens: {stats['total_tokens']}")
