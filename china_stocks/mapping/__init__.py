"""
公告分类体系（单一数据源，所有脚本引用这里）。

对应 crypto 项目的 taxonomy.py，但面向 A 股公告做定制。

两个正交维度：
  DOC_TYPES     — 公告大类（announcement / research / survey / prospectus / ...）
  CONTENT_TAGS  — 内容标签（多标签，更细的投研维度，20+ 类）

L1 规则分类（免费、确定性）：按关键词匹配标题。
L2 AI 分类（可选）：对模糊分类调用 LLM 读正文判断。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# ============================================================
# 公告大类（doc_type，单值）
# ============================================================
DOC_TYPES = {
    "announcement": "公司公告",       # 最广义的公告，兜底
    "annual_report": "年报",           # 年度报告
    "semi_annual_report": "半年报",    # 半年度报告
    "quarterly_report": "季报",        # 季度报告
    "prospectus": "招股说明书",        # IPO / 增发招股
    "research": "券商研报",           # 第三方研报
    "survey": "调研纪要",             # 机构调研
    "listing": "上市公告书",
    "other": "其他",
}

# ============================================================
# 内容标签（content_topics，多标签，22 类，对齐 A 股投研维度）
# ============================================================
CONTENT_TOPICS = {
    # 财报类
    "annual_report": "年报",
    "semi_annual_report": "半年报",
    "quarterly_report": "季报",
    "earnings_forecast": "业绩预告",
    "earnings_correction": "业绩修正/快报",

    # 资本运作
    "dividend": "分红送转",
    "buyback": "回购",
    "add_issue": "增发/配股",
    "convertible_bond": "可转债",
    "merger": "并购重组",
    "ipo": "IPO/上市",

    # 股本变动
    "unlock": "限售股解禁",
    "share_change": "增减持",
    "equity_pledge": "股权质押",
    "restricted_share": "股权激励/员工持股",

    # 重大事项
    "major_event": "重大事项",
    "litigation": "诉讼/仲裁",
    "investigation": "立案调查/处罚",
    "st_change": "ST/摘帽",

    # 公司治理
    "management_change": "高管变动",
    "shareholder_change": "股东变动/举牌",

    # 经营/行业
    "business": "经营/日常公告",
    "government_grant": "政府补助",
    "contract": "重大合同",
    "related_transaction": "关联交易",

    # 其他
    "other": "其他",
}

# ============================================================
# L1 规则：标题关键词 → 内容标签
# 优先级按列表顺序，命中即停（doc_type 是单值分类）
# ============================================================

# doc_type 判定关键词（按优先级从高到低）
DOC_TYPE_RULES: list[tuple[str, list[str]]] = [
    ("annual_report", ["年度报告", "年报"]),
    ("semi_annual_report", ["半年度报告", "半年报"]),
    ("quarterly_report", ["季度报告", "一季报", "三季报", "季报"]),
    ("prospectus", ["招股说明书", "招股意向书", "募集说明书"]),
    ("listing", ["上市公告书"]),
    ("survey", ["调研纪要", "调研记录", "投资者关系活动记录表", "机构调研"]),
    ("research", ["研报", "研究报告", "深度报告", "点评报告"]),
]

# content_topics 多标签规则（命中多个关键词则有多个标签）
CONTENT_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "annual_report": ["年度报告", "年报"],
    "semi_annual_report": ["半年度报告", "半年报"],
    "quarterly_report": ["季度报告", "一季报", "三季报", "季报"],
    "earnings_forecast": ["业绩预告", "盈利预测", "业绩预测"],
    "earnings_correction": ["业绩快报", "业绩修正", "业绩更正", "业绩预告修正"],
    "dividend": ["分红", "派息", "送转", "利润分配", "权益分派", "转增"],
    "buyback": ["回购", "股份回购", "回购股份"],
    "add_issue": ["增发", "配股", "非公开发行", "定向增发"],
    "convertible_bond": ["可转债", "可转换公司债券"],
    "merger": ["重大资产重组", "并购", "重组", "吸收合并"],
    "ipo": ["首次公开发行", "上市公告", "招股", "发行公告"],
    "unlock": ["限售股上市流通", "解禁", "解除限售"],
    "share_change": ["增持", "减持", "股东减持", "股东增持", "集中竞价交易减持"],
    "equity_pledge": ["股权质押", "股份质押", "解除质押"],
    "restricted_share": ["股权激励", "员工持股计划", "限制性股票"],
    "major_event": ["重大事项", "重大合同", "重大投资", "重大诉讼", "关联交易", "对外担保"],
    "litigation": ["诉讼", "仲裁"],
    "investigation": ["立案调查", "行政处罚", "监管警示", "收到监管函", "问询函"],
    "st_change": ["实施其他风险警示", "撤销风险警示", "ST", "摘帽", "退市风险警示"],
    "management_change": ["董事长", "总经理", "董事变动", "高管变动", "辞职", "聘任"],
    "shareholder_change": ["举牌", "要约收购", "协议转让", "权益变动", "第一大股东变更"],
    "business": ["日常经营", "经营情况", "产品", "中标", "项目进展"],
    "government_grant": ["政府补助", "政府补贴", "收到补助"],
    "contract": ["重大合同", "框架协议", "合作协议"],
    "related_transaction": ["关联交易", "关联方"],
}


@dataclass
class ClassifyResult:
    doc_type: str
    content_topics: list[str]
    method: str          # rule / ai
    confidence: float    # 0.0 ~ 1.0


def classify_announcement(title: str) -> ClassifyResult:
    """
    L1 规则分类：根据公告标题判断 doc_type + content_topics。

    置信度：
      - 命中 doc_type 规则 → 0.9
      - 仅命中 content_topic 关键词 → 0.6
      - 都没命中，兜底 announcement → 0.3
    """
    title_clean = re.sub(r"\s+", "", title or "")

    # 1. doc_type（单值，按优先级匹配）
    doc_type = "announcement"
    dt_confidence = 0.3
    for dt, keywords in DOC_TYPE_RULES:
        if any(kw in title_clean for kw in keywords):
            doc_type = dt
            dt_confidence = 0.9
            break

    # 2. content_topics（多标签）
    topics: list[str] = []
    for topic, keywords in CONTENT_TOPIC_KEYWORDS.items():
        if any(kw in title_clean for kw in keywords):
            topics.append(topic)

    # 3. 综合置信度
    if dt_confidence >= 0.9:
        confidence = 0.9
    elif topics:
        confidence = 0.6
    else:
        confidence = 0.3
        topics = ["other"]

    # doc_type 对应的主 topic 也加进去
    if doc_type in CONTENT_TOPICS and doc_type not in topics:
        topics.insert(0, doc_type)

    return ClassifyResult(
        doc_type=doc_type,
        content_topics=topics,
        method="rule",
        confidence=confidence,
    )


def get_topic_priority(topic: str) -> int:
    """投研关注度优先级（数值越高越重要），用于排序和筛选。"""
    high = {"annual_report", "semi_annual_report", "earnings_forecast",
            "merger", "buyback", "unlock", "investigation", "st_change",
            "shareholder_change", "major_event", "ipo"}
    medium = {"quarterly_report", "earnings_correction", "dividend",
              "add_issue", "convertible_bond", "share_change",
              "equity_pledge", "restricted_share", "litigation",
              "management_change", "contract", "related_transaction"}
    if topic in high:
        return 3
    if topic in medium:
        return 2
    return 1


def get_topic_label(topic: str) -> str:
    """返回 topic 的中文名称。"""
    return CONTENT_TOPICS.get(topic, topic)
