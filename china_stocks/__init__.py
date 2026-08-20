"""
A股投研资料采集系统 (China Stocks Research Collection)

参考 crypto-profile-collection 的分层架构，面向 A 股做定制化适配：
  raw   原始响应层（API 返回的原始 JSON/HTML）
  src   来源解析层（akshare 等数据源 → 结构化中间表）
  core  统一实体层（股票主表、主行业映射）
  biz   业务消费层（行情、财务、资金面、公告、研报……）
  sys   系统元数据（采集运行记录、数据源配置）
"""

__version__ = "0.1.0"
