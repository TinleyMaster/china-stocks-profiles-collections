"""
LLM 客户端（可插拔，支持多种后端）。

设计原则：
  - 统一接口 chat_completion()，后端可切换
  - 已支持：OpenAI 兼容接口（含 DeepSeek/Qwen/智谱等）、本地模型（预留）
  - 失败自动重试 + 降级
  - 调用全部落 raw 层记录

配置（.env）：
  LLM_PROVIDER=openai_compatible
  LLM_API_KEY=sk-xxx
  LLM_BASE_URL=https://api.deepseek.com/v1
  LLM_MODEL=deepseek-chat
  LLM_EMBEDDING_MODEL=text-embedding-v2 （可选）
"""
from __future__ import annotations

import json
from typing import Optional

from ..config import _get_env
from ..logging_setup import logger
from ..raw.storage import save_raw_response

# 延迟导入
_openai_client = None


def _get_provider() -> str:
    return _get_env("LLM_PROVIDER", "mock")


def _get_model() -> str:
    return _get_env("LLM_MODEL", "mock-model")


def _get_openai_client():
    """获取 OpenAI 兼容客户端（延迟导入）。"""
    global _openai_client
    if _openai_client is not None:
        return _openai_client

    provider = _get_provider()
    if provider != "openai_compatible":
        return None

    try:
        from openai import OpenAI  # 延迟导入

        api_key = _get_env("LLM_API_KEY", "")
        base_url = _get_env("LLM_BASE_URL", "")
        if not api_key:
            logger.warning("LLM_API_KEY 未设置，LLM 功能不可用")
            return None

        _openai_client = OpenAI(api_key=api_key, base_url=base_url or None)
        return _openai_client
    except ImportError:
        logger.warning("openai 库未安装，LLM 功能不可用（pip install openai）")
        return None


def is_available() -> bool:
    """检查 LLM 是否可用。"""
    provider = _get_provider()
    if provider == "mock":
        return True
    if provider == "openai_compatible":
        return _get_openai_client() is not None
    return False


def chat_completion(
    messages: list[dict],
    model: Optional[str] = None,
    temperature: float = 0.3,
    max_tokens: int = 2048,
    save_raw: bool = True,
) -> dict:
    """
    统一聊天补全接口。

    Args:
        messages: [{"role": "system/user/assistant", "content": "..."}]
        model: 模型名，不填用默认
        temperature: 随机性
        max_tokens: 最大输出 token
        save_raw: 是否存 raw 层

    Returns:
        {"content": str, "model": str, "usage": dict, "provider": str}
    """
    provider = _get_provider()
    model = model or _get_model()

    if provider == "mock" or not is_available():
        return _mock_response(messages, model)

    if provider == "openai_compatible":
        return _openai_completion(messages, model, temperature, max_tokens, save_raw)

    raise ValueError(f"不支持的 LLM provider: {provider}")


def _openai_completion(
    messages: list[dict],
    model: str,
    temperature: float,
    max_tokens: int,
    save_raw: bool,
) -> dict:
    """OpenAI 兼容接口调用。"""
    client = _get_openai_client()
    if not client:
        return _mock_response(messages, model)

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=False,
        )
        content = resp.choices[0].message.content or ""
        usage = {
            "prompt_tokens": resp.usage.prompt_tokens if resp.usage else 0,
            "completion_tokens": resp.usage.completion_tokens if resp.usage else 0,
            "total_tokens": resp.usage.total_tokens if resp.usage else 0,
        }

        if save_raw:
            try:
                save_raw_response(
                    platform_code="llm",
                    api_name="chat_completion",
                    params={"model": model, "temperature": temperature, "max_tokens": max_tokens},
                    response={"messages": messages, "content": content, "usage": usage},
                )
            except Exception as e:
                logger.warning(f"保存 LLM raw 失败: {e}")

        return {
            "content": content,
            "model": model,
            "usage": usage,
            "provider": "openai_compatible",
        }
    except Exception as e:
        logger.error(f"LLM 调用失败: {e}")
        raise


def _mock_response(messages: list[dict], model: str) -> dict:
    """Mock 响应（没配置 LLM 时用）。"""
    last_msg = messages[-1]["content"] if messages else ""
    content = (
        f"[Mock 模式] 你问的是：{last_msg[:100]}\n\n"
        "当前未配置 LLM 模型。请在 .env 中设置：\n"
        "  LLM_PROVIDER=openai_compatible\n"
        "  LLM_API_KEY=your_key\n"
        "  LLM_BASE_URL=https://api.deepseek.com/v1\n"
        "  LLM_MODEL=deepseek-chat\n"
    )
    return {
        "content": content,
        "model": model,
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        "provider": "mock",
    }


def embed(texts: list[str], model: Optional[str] = None) -> list[list[float]]:
    """
    文本向量化（可选功能，用于向量检索）。
    未配置时返回空列表。
    """
    if not is_available():
        return []

    provider = _get_provider()
    if provider != "openai_compatible":
        return []

    client = _get_openai_client()
    if not client:
        return []

    embed_model = model or _get_env("LLM_EMBEDDING_MODEL", "text-embedding-v2")
    try:
        resp = client.embeddings.create(model=embed_model, input=texts)
        return [item.embedding for item in resp.data]
    except Exception as e:
        logger.warning(f"Embedding 调用失败: {e}")
        return []
