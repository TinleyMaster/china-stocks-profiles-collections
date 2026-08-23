"""akshare 封装与公共工具。

设计原则：
- 所有 akshare 调用都走这里，上层脚本不直接 import akshare，方便未来替换数据源。
- 调用异常统一重试 + 记录 raw 响应。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta
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


# ============================================================
# 自研东财直连（公告 / 研报 / 机构调研）
#
# 背景：akshare 1.18.94 对这三组接口做了破坏性变更
#   - stock_notice_cninfo 已被移除（AttributeError）
#   - stock_research_report_em 签名改为 (symbol='000001')，不再接受 date 参数（TypeError）
#   - stock_jgdy_tj_em 签名仍在，但其 datacenter-web 过滤器在部分环境下不稳定
# 因此改为自研直连东财 API，返回与上层 phase 脚本 _find_columns 期望一致的中文列名
# DataFrame，最小化改动面。复用项目既有的 Session + 浏览器 UA + 退避重试模式。
# ============================================================

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _em_get_json(sess: requests.Session, url: str, params: dict, max_try: int = 8) -> dict:
    """东财接口通用 GET + 退避重试（兼容 SOCKS 代理偶发 TLS 中断）。

    requests.Session 默认读取 HTTP_PROXY/HTTPS_PROXY 环境变量，
    生产环境直连、本地走代理均无需硬编码。
    """
    last_err: Exception | None = None
    for attempt in range(max_try):
        try:
            r = sess.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001 - 网络异常统一退避重试
            last_err = e
            time.sleep(1.5 + attempt * 0.5)
    raise ConnectionError(f"东财接口拉取失败 {url}: {last_err}")


def fetch_announcements_by_date(
    trade_date: str,
    category: str = "全部",
    save_raw: bool = True,
) -> pd.DataFrame:
    """巨潮资讯/东财-个股公告（自研直连，替代已移除的 akshare.stock_notice_cninfo）。

    返回与 phase_b2_announcements._find_columns 匹配的中文列：
        股票代码 / 股票简称 / 公告标题 / 公告日期 / 公告类型 / 公告链接
    trade_date 格式 YYYYMMDD。
    """
    ymd = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
    base_params = {
        "sr": "-1",
        "page_size": "100",
        "ann_type": "A",
        "client_source": "web",
        "stock_list": "",
        "f_node": "0",
        "s_node": "0",
        "date": ymd,
    }
    sess = requests.Session()
    sess.headers.update({"User-Agent": BROWSER_UA})

    rows: list[dict] = []
    max_pages = 100  # page_size=100 → 单日最多 10,000 条，避免极端披露日静默截断
    page = 0
    while page < max_pages:
        params = {**base_params, "page_index": str(page)}
        try:
            j = _em_get_json(sess, url, params)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"公告拉取失败 date={trade_date} page={page}: {e}")
            break
        lst = (j.get("data") or {}).get("list") or []
        if not lst:
            break
        for rec in lst:
            codes = rec.get("codes") or []
            if not codes:
                continue
            code_info = codes[0]
            cols = rec.get("columns") or []
            notice_date = str(rec.get("notice_date") or "")[:10]
            art_code = rec.get("art_code", "")
            ann_type_name = cols[0].get("column_name", "") if cols else ""
            ann_link = (
                f"https://www.cninfo.com.cn/new/disclosure/detail?"
                f"announcementId={art_code}&announcementTime={notice_date}"
            )
            rows.append({
                "股票代码": str(code_info.get("stock_code", "")).strip(),
                "股票简称": str(code_info.get("short_name", "")).strip(),
                "公告标题": str(rec.get("title", "")).strip(),
                "公告日期": notice_date,
                "公告类型": str(ann_type_name).strip(),
                "公告链接": ann_link,
            })
        if len(lst) < 100:
            break
        if page + 1 >= max_pages:
            # 末页仍满页：单日公告数已触 10,000 上限，继续翻页无意义，告警后停止以防静默截断
            logger.warning(
                f"公告拉取 date={trade_date} 已达分页上限 {max_pages * 100} 条，"
                f"单日公告可能超过上限被截断，请关注"
            )
            break
        page += 1
        time.sleep(0.2)

    df = pd.DataFrame(rows)
    if save_raw:
        try:
            save_raw_response(
                platform_code="eastmoney",
                api_name="stock_notice_em",
                params={"date": trade_date, "category": category},
                response=df.to_dict(orient="records"),
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"保存公告 raw 响应失败: {e}")
    logger.info(f"{trade_date} 公告拉取: {len(df)} 条")
    return df


def fetch_research_reports_by_date(
    trade_date: str,
    save_raw: bool = True,
) -> pd.DataFrame:
    """东财研报中心（自研直连，替代签名变更的 akshare.stock_research_report_em）。

    返回与 phase_b3_research._find_columns 匹配的中文列：
        股票代码 / 股票简称 / 报告标题 / 券商 / 发布日期 / 评级 / 研报链接
    trade_date 格式 YYYYMMDD。
    """
    ymd = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    url = "https://reportapi.eastmoney.com/report/list"
    base_params = {
        "industryCode": "*",
        "pageSize": "100",
        "industry": "*",
        "rating": "*",
        "ratingChange": "*",
        "beginTime": ymd,
        "endTime": ymd,
        "pageNo": "1",
        "fields": "",
        "qType": "0",
    }
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": BROWSER_UA,
        "Referer": "https://data.eastmoney.com/report/",
    })

    rows: list[dict] = []
    page = 1
    while page < 100:
        params = {**base_params, "pageNo": str(page)}
        try:
            j = _em_get_json(sess, url, params)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"研报拉取失败 date={trade_date} page={page}: {e}")
            break
        lst = j.get("data") or []
        if not lst:
            break
        for rec in lst:
            pub = str(rec.get("publishDate") or "")[:10]
            info_code = rec.get("infoCode", "")
            rep_link = (
                f"https://data.eastmoney.com/report/{pub.replace('-', '')}/{info_code}.html"
                if info_code else ""
            )
            broker = rec.get("orgSName") or rec.get("orgName") or ""
            rows.append({
                "股票代码": str(rec.get("stockCode", "")).strip(),
                "股票简称": str(rec.get("stockName", "")).strip(),
                "报告标题": str(rec.get("title", "")).strip(),
                "券商": str(broker).strip(),
                "发布日期": pub,
                "评级": str(rec.get("rating") or "").strip(),
                "研报链接": rep_link,
            })
        if len(lst) < 100:
            break
        page += 1
        time.sleep(0.3)

    df = pd.DataFrame(rows)
    if save_raw:
        try:
            save_raw_response(
                platform_code="eastmoney",
                api_name="stock_research_report_em",
                params={"date": trade_date},
                response=df.to_dict(orient="records"),
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"保存研报 raw 响应失败: {e}")
    logger.info(f"{trade_date} 研报拉取: {len(df)} 条")
    return df


def fetch_survey_stat_by_date(
    trade_date: str,
    save_raw: bool = False,
) -> pd.DataFrame:
    """东财机构调研统计（自研直连，替代 akshare.stock_jgdy_tj_em）。

    东财 datacenter-web 的 NOTICE_DATE 过滤仅 `>` 生效（= / >= / <= 被忽略），
    且 `>` 为严格大于会漏掉 trade_date 当天记录，故查询条件用 trade_date 的
    前一日（> 前一日）以包含当天，再在 Python 侧精确筛出 trade_date 当天记录。

    返回与 phase_b3_survey._find_columns 匹配的中文列：
        股票代码 / 股票简称 / 公告日期 / 调研方式 / 接待人员 / 接待机构数量
    trade_date 格式 YYYYMMDD。
    """
    prev_ymd = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    base_params = {
        "sortColumns": "NOTICE_DATE,SUM,RECEIVE_START_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1,-1,1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_ORG_SURVEYNEW",
        "columns": "ALL",
        "quoteColumns": "f2~01~SECURITY_CODE~CLOSE_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE",
        "source": "WEB",
        "client": "WEB",
        "filter": f"""(NUMBERNEW="1")(IS_SOURCE="1")(NOTICE_DATE>'{prev_ymd}')""",
    }
    sess = requests.Session()
    sess.headers.update({"User-Agent": BROWSER_UA})

    rows: list[dict] = []
    try:
        first = _em_get_json(sess, url, base_params)
        res = first.get("result") or {}
        total_pages = int(res.get("pages", 1) or 1)
        pages_data = [res.get("data") or []]
        for pg in range(2, total_pages + 1):
            p = {**base_params, "pageNumber": str(pg)}
            try:
                j2 = _em_get_json(sess, url, p)
                pages_data.append((j2.get("result") or {}).get("data") or [])
            except Exception as e:  # noqa: BLE001
                logger.warning(f"调研分页拉取失败 date={trade_date} page={pg}: {e}")
                break
        for pg_data in pages_data:
            for rec in pg_data:
                nd = str(rec.get("NOTICE_DATE", ""))[:10].replace("-", "")
                if nd != trade_date:
                    continue
                rows.append({
                    "股票代码": str(rec.get("SECURITY_CODE", "")).strip(),
                    "股票简称": str(rec.get("SECURITY_NAME_ABBR", "")).strip(),
                    "公告日期": str(rec.get("NOTICE_DATE", ""))[:10],
                    "调研方式": str(rec.get("RECEIVE_WAY_EXPLAIN") or "").strip(),
                    "接待人员": str(rec.get("RECEPTIONIST") or "").strip(),
                    "接待机构数量": rec.get("NUM"),
                })
    except Exception as e:  # noqa: BLE001
        logger.warning(f"调研拉取失败 date={trade_date}: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if save_raw:
        try:
            save_raw_response(
                platform_code="eastmoney",
                api_name="stock_jgdy_tj_em",
                params={"date": trade_date},
                response=df.to_dict(orient="records"),
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"保存调研 raw 响应失败: {e}")
    logger.info(f"{trade_date} 调研拉取: {len(df)} 条")
    return df
