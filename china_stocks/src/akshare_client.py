"""akshare 封装与公共工具。

设计原则：
- 所有 akshare 调用都走这里，上层脚本不直接 import akshare，方便未来替换数据源。
- 调用异常统一重试 + 记录 raw 响应。
"""
from __future__ import annotations

import time
from typing import Any, Callable

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..logging_setup import logger
from ..raw.storage import save_raw_response

# 延迟导入 akshare，减少启动时间
_ak = None


def _get_ak():
    global _ak
    if _ak is None:
        import akshare as ak  # noqa: WPS433

        _ak = ak
    return _ak


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, RuntimeError)),
    reraise=True,
)
def call_api(api_name: str, save_raw: bool = True, **kwargs) -> pd.DataFrame:
    """
    统一调用 akshare 接口。

    - 自动重试网络错误
    - 记录原始响应到 raw 层（可配置）
    - 返回 DataFrame

    Args:
        api_name: akshare 的函数名，如 "stock_info_a_code_name"
        save_raw: 是否保存原始响应到 raw.api_response
        **kwargs: 传给 akshare 接口的参数
    """
    ak = _get_ak()
    func = getattr(ak, api_name, None)
    if func is None:
        raise AttributeError(f"akshare 中不存在接口: {api_name}")

    logger.debug(f"调用 akshare.{api_name} 参数={kwargs}")
    t0 = time.time()
    try:
        df: pd.DataFrame = func(**kwargs)
    except Exception as e:
        logger.warning(f"akshare.{api_name} 调用失败: {e}")
        raise

    cost = time.time() - t0
    logger.debug(f"akshare.{api_name} 返回 {len(df)} 行，耗时 {cost:.2f}s")

    if save_raw:
        try:
            save_raw_response(
                platform_code="akshare",
                api_name=api_name,
                params=kwargs,
                response=df.to_dict(orient="records"),
            )
        except Exception as e:
            logger.warning(f"保存 raw 响应失败: {e}")

    return df
