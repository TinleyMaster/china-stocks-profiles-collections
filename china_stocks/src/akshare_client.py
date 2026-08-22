"""akshare 封装与公共工具。

设计原则：
- 所有 akshare 调用都走这里，上层脚本不直接 import akshare，方便未来替换数据源。
- 调用异常统一重试 + 记录 raw 响应。
"""
from __future__ import annotations

import time
from io import StringIO
from typing import Any, Callable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..logging_setup import logger
from ..raw.storage import save_raw_response

# 抑制 pandas 对 object dtype 列 ffill 的 FutureWarning（新浪股东/财务解析大量触发）
pd.set_option("future.no_silent_downcasting", True)

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


# 新浪财务指标共享 Session（连接复用，避免逐只新建连接被限流重置）
_finance_session: requests.Session | None = None


def _get_finance_session() -> requests.Session:
    global _finance_session
    if _finance_session is None:
        _finance_session = requests.Session()
        _finance_session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            )
        })
    return _finance_session


def fetch_finance_indicator(symbol: str, start_year: str = "1900") -> pd.DataFrame:
    """新浪财经-财务分析-财务指标（自研，共享 Session + 退避重试）。

    等价于 akshare.stock_financial_analysis_indicator，但复用同一个
    requests.Session，避免逐只新建 TCP 连接触发新浪限流（SSL EOF）。
    返回列结构与 akshare 一致：第一列「日期」，其余为中文指标列。
    """
    sess = _get_finance_session()
    base = "https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine"

    def _get(url: str) -> requests.Response:
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                r = sess.get(url, timeout=30)
                r.raise_for_status()
                return r
            except Exception as e:  # noqa: BLE001 - 网络异常统一退避重试
                last_err = e
                time.sleep(1 + attempt * 2)
        raise ConnectionError(f"新浪财务指标拉取失败 {url}: {last_err}")

    r = _get(f"{base}/stockid/{symbol}/ctrl/2020/displaytype/4.phtml")
    soup = BeautifulSoup(r.text, features="lxml")
    year_container = soup.find(attrs={"id": "con02-1"})
    if year_container is None or year_container.find("table") is None:
        return pd.DataFrame()
    year_context = year_container.find("table").find_all("a")
    year_list = [item.text.strip() for item in year_context if item.text.strip()]
    if not year_list:
        return pd.DataFrame()

    if start_year.isdigit():
        year_list = [item for item in year_list if item.isdigit() and item >= start_year]
    elif start_year in year_list:
        year_list = year_list[: year_list.index(start_year) + 1]
    else:
        return pd.DataFrame()
    if not year_list:
        return pd.DataFrame()

    out_df = pd.DataFrame()
    indicator_list = [
        "每股指标", "盈利能力", "成长能力", "营运能力",
        "偿债及资本结构", "现金流量", "其他指标",
    ]
    for year_item in year_list:
        url = f"{base}/stockid/{symbol}/ctrl/{year_item}/displaytype/4.phtml"
        r = _get(url)
        temp_df = pd.read_html(StringIO(r.text))[12].iloc[:, :-1]
        temp_df.columns = temp_df.iloc[0, :]
        temp_df = temp_df.iloc[1:, :]
        big_df = pd.DataFrame()
        for i in range(len(indicator_list)):
            if i == 6:
                inner_df = temp_df[
                    temp_df.loc[
                        temp_df.iloc[:, 0].str.find(indicator_list[i]) == 0, :
                    ].index[0]:
                ].T
            else:
                inner_df = temp_df[
                    temp_df.loc[
                        temp_df.iloc[:, 0].str.find(indicator_list[i]) == 0, :
                    ].index[0]: temp_df.loc[
                        temp_df.iloc[:, 0].str.find(indicator_list[i + 1]) == 0, :
                    ].index[0] - 1
                ].T
            inner_df = inner_df.reset_index(drop=True)
            big_df = pd.concat(objs=[big_df, inner_df], axis=1)
        big_df.columns = big_df.iloc[0, :].tolist()
        big_df = big_df.iloc[1:, :]
        big_df.index = temp_df.columns.tolist()[1:]
        out_df = pd.concat(objs=[out_df, big_df])

    out_df.dropna(inplace=True)
    out_df.reset_index(inplace=True)
    out_df.rename(columns={"index": "日期"}, inplace=True)
    out_df.sort_values(by=["日期"], ignore_index=True, inplace=True)
    out_df["日期"] = pd.to_datetime(out_df["日期"], errors="coerce").dt.date
    for item in out_df.columns[1:]:
        out_df[item] = pd.to_numeric(out_df[item], errors="coerce")
    return out_df


def fetch_main_stock_holder(symbol: str) -> pd.DataFrame:
    """新浪财经-股本股东-主要股东（自研，共享 Session + 退避重试）。

    等价于 akshare.stock_main_stock_holder，但复用同一个 requests.Session。
    返回列：编号/股东名称/持股数量/持股比例/股本性质/截至日期/公告日期/股东说明/股东总数/平均持股数，
    多报告期纵向堆叠，最新一期在最前。
    """
    sess = _get_finance_session()
    url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/{symbol}.phtml"

    last_err: Exception | None = None
    r = None
    for attempt in range(3):
        try:
            r = sess.get(url, timeout=30)
            r.raise_for_status()
            break
        except Exception as e:  # noqa: BLE001 - 网络异常统一退避重试
            last_err = e
            time.sleep(1 + attempt * 2)
    if r is None:
        raise ConnectionError(f"新浪主要股东拉取失败 {url}: {last_err}")

    temp_df = pd.read_html(StringIO(r.text))[13].iloc[:, :5]
    temp_df.columns = [*range(5)]
    big_df = pd.DataFrame()
    need_range = (
        temp_df[temp_df.iloc[:, 0].str.find("截至日期") == 0].index.tolist()
        + [len(temp_df)]
    )
    for i in range(len(need_range) - 1):
        truncated_df = temp_df.iloc[need_range[i]: need_range[i + 1], :]
        truncated_df = truncated_df.dropna(how="all")
        temp_truncated = truncated_df.iloc[5:, :]
        temp_truncated.reset_index(inplace=True, drop=True)
        concat_df = pd.concat(
            objs=[
                temp_truncated,
                truncated_df.iloc[0, :],
                truncated_df.iloc[1, :],
                truncated_df.iloc[2, :],
                truncated_df.iloc[3, :],
                truncated_df.iloc[4, :],
            ],
            axis=1,
        )
        concat_df.columns = concat_df.iloc[0, :]
        concat_df = concat_df.iloc[1:, :].copy()
        concat_df["截至日期"] = concat_df["截至日期"].ffill()
        concat_df["公告日期"] = concat_df["公告日期"].ffill()
        concat_df["股东总数"] = concat_df["股东总数"].ffill()
        concat_df["平均持股数"] = concat_df["平均持股数"].ffill()
        concat_df["股东总数"] = concat_df["股东总数"].astype(str).str.strip("查看变化趋势")
        concat_df["平均持股数"] = concat_df["平均持股数"].astype(str).str.strip(
            "(按总股本计算) 查看变化趋势"
        )
        big_df = pd.concat(objs=[big_df, concat_df], axis=0, ignore_index=True)

    big_df.dropna(inplace=True, how="all")
    big_df.reset_index(inplace=True, drop=True)
    big_df.rename(
        columns={"持股数量(股)": "持股数量", "持股比例(%)": "持股比例"}, inplace=True
    )
    big_df.columns.name = None
    big_df["持股数量"] = pd.to_numeric(big_df["持股数量"], errors="coerce")
    big_df["持股比例"] = big_df["持股比例"].astype(str).str.strip("↓")
    big_df["持股比例"] = pd.to_numeric(big_df["持股比例"], errors="coerce")
    big_df["截至日期"] = pd.to_datetime(big_df["截至日期"], errors="coerce").dt.date
    big_df["公告日期"] = pd.to_datetime(big_df["公告日期"], errors="coerce").dt.date
    big_df["股东总数"] = pd.to_numeric(big_df["股东总数"], errors="coerce")
    big_df["平均持股数"] = pd.to_numeric(big_df["平均持股数"], errors="coerce")
    return big_df
