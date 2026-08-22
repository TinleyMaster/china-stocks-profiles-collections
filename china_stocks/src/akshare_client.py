"""akshare 封装与公共工具。

设计原则：
- 所有 akshare 调用都走这里，上层脚本不直接 import akshare，方便未来替换数据源。
- 调用异常统一重试 + 记录 raw 响应。
"""
from __future__ import annotations

import time
from typing import Any, Callable

import pandas as pd
import requests
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


def fetch_tx_spot_snapshot(save_raw: bool = True) -> pd.DataFrame:
    """腾讯证券-沪深京全市场实时快照（含 PE_TTM/PB/总市值/流通市值）。

    自研分页实现，比 akshare.stock_zh_a_spot_tx 更稳定：
    - Session 连接复用 + 浏览器 UA，避免被腾讯限流重置（SSL EOF）
    - 页间间隔 0.3s，单页失败退避重试 3 次
    返回列结构与 akshare 输出一致（保留 code/zxj/zdf/hsl/pe_ttm/pn/zsz/ltsz 等原始字段）。
    """
    url = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
    page_size = 200
    base_params = {
        "_appver": "11.17.0",
        "board_code": "aStock",
        "sort_type": "price",
        "direct": "down",
        "count": str(page_size),
    }
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
    })

    def _get_page(offset: int) -> tuple[list[dict], int]:
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                r = sess.get(url, params={**base_params, "offset": str(offset)}, timeout=30)
                r.raise_for_status()
                data = r.json()["data"]
                return data["rank_list"], int(data["total"])
            except Exception as e:  # noqa: BLE001 - 网络异常统一退避重试
                last_err = e
                time.sleep(1 + attempt * 2)
        raise ConnectionError(f"腾讯行情快照拉取失败 offset={offset}: {last_err}")

    t0 = time.time()
    rows: list[dict] = []
    first, total = _get_page(0)
    rows.extend(first)
    for offset in range(page_size, total, page_size):
        page_rows, _ = _get_page(offset)
        rows.extend(page_rows)
        time.sleep(0.3)

    df = pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)
    logger.info(f"腾讯行情快照拉取完成: {len(df)} 行，耗时 {time.time() - t0:.1f}s")

    if save_raw:
        try:
            save_raw_response(
                platform_code="akshare",
                api_name="stock_zh_a_spot_tx",
                params={"source": "tencent_direct", "total": total},
                response=df.to_dict(orient="records"),
            )
        except Exception as e:
            logger.warning(f"保存 raw 响应失败: {e}")

    return df
