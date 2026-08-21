"""
数据库初始化 DDL —— 分层设计：
  sys   系统元数据层
  raw   原始响应层
  src   来源解析层（src_akshare）
  core  统一实体层
  biz   业务消费层

所有表统一带 created_at / updated_at 字段。
"""

INIT_SQL = """
-- ============================================================
-- 0. 扩展
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- 1. sys —— 系统元数据
-- ============================================================
CREATE TABLE IF NOT EXISTS sys.source_platform (
    platform_code   TEXT PRIMARY KEY,          -- akshare / eastmoney / cninfo ...
    platform_name   TEXT NOT NULL,
    base_url        TEXT,
    is_free         BOOLEAN DEFAULT TRUE,
    remark          TEXT
);

CREATE TABLE IF NOT EXISTS sys.ingest_run (
    run_id          BIGSERIAL PRIMARY KEY,
    platform_code   TEXT NOT NULL,             -- 来源平台
    phase           TEXT NOT NULL,             -- phase_a / phase_b1 / phase_c_finance ...
    target          TEXT,                      -- 本次采集目标（股票代码列表或范围描述）
    status          TEXT NOT NULL DEFAULT 'running',  -- running / success / failed
    rows_inserted   INTEGER DEFAULT 0,
    rows_updated    INTEGER DEFAULT 0,
    error_msg       TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    cost_seconds    NUMERIC(10, 2)
);

CREATE INDEX IF NOT EXISTS idx_ingest_run_phase ON sys.ingest_run(phase, started_at DESC);

-- ============================================================
-- 2. raw —— 原始响应层
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.api_response (
    id              BIGSERIAL PRIMARY KEY,
    platform_code   TEXT NOT NULL,
    api_name        TEXT NOT NULL,             -- akshare 的函数名或接口标识
    params          JSONB,                     -- 调用参数
    response        JSONB,                     -- 原始响应（JSON 化后）
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_api_lookup ON raw.api_response(platform_code, api_name, fetched_at DESC);

-- ============================================================
-- 3. src —— 来源解析层（akshare）
-- ============================================================
-- 全 A 股列表快照（akshare.stock_info_a_code_name / stock_zh_a_spot_em）
CREATE TABLE IF NOT EXISTS src_akshare.stock_list (
    stock_code      TEXT PRIMARY KEY,          -- 6 位代码
    stock_name      TEXT NOT NULL,
    market          TEXT,                      -- SH / SZ / BJ
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 申万行业分类快照
CREATE TABLE IF NOT EXISTS src_akshare.sw_industry (
    id              BIGSERIAL PRIMARY KEY,
    stock_code      TEXT NOT NULL,
    stock_name      TEXT NOT NULL,
    industry_l1     TEXT,                      -- 申万一级
    industry_l2     TEXT,                      -- 申万二级
    industry_l3     TEXT,                      -- 申万三级
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sw_industry_code ON src_akshare.sw_industry(stock_code);

-- 日线行情快照（增量追加）
CREATE TABLE IF NOT EXISTS src_akshare.stock_daily (
    stock_code      TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    open            NUMERIC(12, 4),
    high            NUMERIC(12, 4),
    low             NUMERIC(12, 4),
    close           NUMERIC(12, 4),
    volume          BIGINT,                    -- 成交量（手）
    amount          NUMERIC(18, 2),            -- 成交额（元）
    amplitude       NUMERIC(8, 4),             -- 振幅 %
    change_pct      NUMERIC(8, 4),             -- 涨跌幅 %
    change_amount   NUMERIC(10, 4),            -- 涨跌额
    turnover_rate   NUMERIC(8, 4),             -- 换手率 %
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_code, trade_date)
);

-- 财务三大表（按报告期）— 简化版
CREATE TABLE IF NOT EXISTS src_akshare.financial_report (
    stock_code      TEXT NOT NULL,
    report_date     DATE NOT NULL,             -- 报告期（如 2024-12-31）
    report_type     TEXT NOT NULL,             -- income / balance / cashflow
    report_json     JSONB NOT NULL,            -- 完整原始字段
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_code, report_date, report_type)
);

-- 关键财务指标（按报告期）
CREATE TABLE IF NOT EXISTS src_akshare.financial_indicator (
    stock_code      TEXT NOT NULL,
    report_date     DATE NOT NULL,
    indicator_json  JSONB NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_code, report_date)
);

-- ============================================================
-- 4. core —— 统一实体层
-- ============================================================
CREATE TABLE IF NOT EXISTS core.stock (
    stock_code      TEXT PRIMARY KEY,          -- 6 位代码（如 600519）
    stock_name      TEXT NOT NULL,
    market          TEXT NOT NULL,             -- SH / SZ / BJ
    full_code       TEXT NOT NULL,             -- 带市场前缀（如 SH600519）
    list_date       DATE,                      -- 上市日期
    primary_industry_l1 TEXT,                  -- 申万一级（主行业）
    primary_industry_l2 TEXT,                  -- 申万二级
    primary_industry_l3 TEXT,                  -- 申万三级
    is_st           BOOLEAN DEFAULT FALSE,     -- 是否 ST
    is_delisted     BOOLEAN DEFAULT FALSE,     -- 是否已退市
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 统一数据源映射表（记录每个股票在各数据源的 id，预留扩展）
CREATE TABLE IF NOT EXISTS core.stock_source_map (
    stock_code      TEXT NOT NULL,
    platform_code   TEXT NOT NULL,
    source_id       TEXT NOT NULL,             -- 该平台下的 id / 代码
    source_name     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_code, platform_code)
);

-- ============================================================
-- 5. biz —— 业务消费层
-- ============================================================
-- 股票基础画像（行情 + 估值的最新快照，日频更新）
CREATE TABLE IF NOT EXISTS biz.stock_basic (
    stock_code      TEXT PRIMARY KEY,
    stock_name      TEXT,
    close           NUMERIC(12, 4),            -- 最新收盘价
    change_pct      NUMERIC(8, 4),             -- 最新涨跌幅
    total_market_cap NUMERIC(18, 2),           -- 总市值（元）
    float_market_cap NUMERIC(18, 2),           -- 流通市值（元）
    pe_ttm          NUMERIC(12, 4),            -- 市盈率 TTM
    pb              NUMERIC(12, 4),            -- 市净率
    ps_ttm          NUMERIC(12, 4),
    dv_ttm          NUMERIC(8, 4),             -- 股息率 TTM %
    turnover_rate   NUMERIC(8, 4),             -- 换手率 %
    volume          BIGINT,
    amount          NUMERIC(18, 2),
    as_of_date      DATE,                      -- 对应交易日
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 财务指标画像（最新报告期 + 滚动 TTM，结构化字段）
CREATE TABLE IF NOT EXISTS biz.finance_snapshot (
    stock_code      TEXT PRIMARY KEY,
    report_date     DATE,                      -- 最新报告期
    revenue         NUMERIC(18, 2),            -- 营业收入
    revenue_yoy     NUMERIC(8, 4),             -- 营收同比 %
    net_profit      NUMERIC(18, 2),            -- 净利润
    net_profit_yoy  NUMERIC(8, 4),             -- 净利同比 %
    roe             NUMERIC(8, 4),             -- ROE %
    roa             NUMERIC(8, 4),             -- ROA %
    gross_margin    NUMERIC(8, 4),             -- 毛利率 %
    net_margin      NUMERIC(8, 4),             -- 净利率 %
    debt_ratio      NUMERIC(8, 4),             -- 资产负债率 %
    current_ratio   NUMERIC(8, 4),             -- 流动比率
    eps             NUMERIC(10, 4),            -- 每股收益
    bps             NUMERIC(10, 4),            -- 每股净资产
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 资金面画像（北向 / 融资融券 / 龙虎榜标记等，日频）
CREATE TABLE IF NOT EXISTS biz.capital_snapshot (
    stock_code      TEXT PRIMARY KEY,
    as_of_date      DATE,
    north_hold_shares   BIGINT,                 -- 北向持股（股）
    north_hold_pct      NUMERIC(8, 4),          -- 北向占流通比 %
    margin_balance      NUMERIC(18, 2),         -- 融资余额（元）
    margin_balance_chg  NUMERIC(18, 2),         -- 融资余额变动
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 股东快照（十大股东，按报告期）
CREATE TABLE IF NOT EXISTS biz.shareholder_snapshot (
    id              BIGSERIAL PRIMARY KEY,
    stock_code      TEXT NOT NULL,
    report_date     DATE NOT NULL,
    top10_json      JSONB NOT NULL,             -- 十大股东明细
    inst_hold_pct   NUMERIC(8, 4),              -- 机构持仓占比
    pledge_pct      NUMERIC(8, 4),              -- 质押比例
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shareholder_code ON biz.shareholder_snapshot(stock_code, report_date DESC);

-- 文档入口（公告 / 研报 / 调研纪要，对应 crypto 的 doc_source_entry）
CREATE TABLE IF NOT EXISTS biz.doc_source_entry (
    id              BIGSERIAL PRIMARY KEY,
    stock_code      TEXT NOT NULL,
    source_platform TEXT NOT NULL,             -- cninfo / eastmoney_research ...
    doc_type        TEXT NOT NULL,             -- announcement / research / survey / prospectus
    sub_type        TEXT,                      -- 子类型（年报/半年报/季报/临时公告...）
    title           TEXT NOT NULL,
    publish_date    DATE,
    url             TEXT NOT NULL,             -- 原文链接
    file_size       BIGINT,
    content_topics  TEXT[] DEFAULT ARRAY[]::TEXT[],  -- 内容标签（多标签）
    classify_method TEXT DEFAULT 'rule',       -- rule / ai
    classify_confidence REAL DEFAULT 0.0,
    is_downloaded   BOOLEAN DEFAULT FALSE,     -- 是否已下载到本地
    file_path       TEXT,                      -- 本地文件路径
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_doc_code_date ON biz.doc_source_entry(stock_code, publish_date DESC);
CREATE INDEX IF NOT EXISTS idx_doc_type ON biz.doc_source_entry(doc_type, publish_date DESC);
CREATE INDEX IF NOT EXISTS idx_doc_url ON biz.doc_source_entry(url);

-- 重要事件（分红/增发/解禁/回购/减持/业绩预告，结构化）
CREATE TABLE IF NOT EXISTS biz.corporate_event (
    id              BIGSERIAL PRIMARY KEY,
    stock_code      TEXT NOT NULL,
    event_type      TEXT NOT NULL,             -- dividend / add_issue / unlock / buyback / reduce / profit_alert / ...
    event_date      DATE,                      -- 事件发生日
    event_data      JSONB NOT NULL,            -- 结构化字段
    source_url      TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_code_type ON biz.corporate_event(stock_code, event_type, event_date DESC);

-- ============================================================
-- Phase D: 投研笔记本（research_notebook / research_message）
-- ============================================================

-- 每股一个投研笔记本
CREATE TABLE IF NOT EXISTS biz.research_notebook (
    stock_code          TEXT PRIMARY KEY,
    stock_name          TEXT,
    industry_l1         TEXT,                           -- 申万一级
    industry_l2         TEXT,
    -- 资料完整性：21 项清单的状态 JSON
    completeness_json   JSONB DEFAULT '{}'::jsonb,      -- {key: {status: done/missing/partial, count: N}}
    total_docs          INTEGER DEFAULT 0,              -- 文档总数
    downloaded_docs     INTEGER DEFAULT 0,              -- 已下载文档数
    total_events        INTEGER DEFAULT 0,              -- 公司事件数
    latest_report_date  DATE,                           -- 最新财报期
    -- 投研笔记（用户可编辑的核心观点）
    thesis              TEXT,                           -- 投资逻辑/核心观点
    tags                TEXT[] DEFAULT ARRAY[]::TEXT[], -- 用户标签
    rating              TEXT,                           -- 自评级（买入/持有/卖出/观察）
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_qa_at          TIMESTAMPTZ                     -- 最后一次问答时间
);

-- 对话消息（RAG 问答历史，严格保留来源引用）
CREATE TABLE IF NOT EXISTS biz.research_message (
    id              BIGSERIAL PRIMARY KEY,
    stock_code      TEXT NOT NULL,
    role            TEXT NOT NULL,             -- user / assistant
    content         TEXT NOT NULL,             -- 消息内容
    sources         JSONB DEFAULT '[]'::jsonb, -- 引用来源 [{doc_id, title, url, snippet}]
    model           TEXT,                      -- 使用的模型
    tokens_used     INTEGER,                   -- token 消耗
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_msg_code ON biz.research_message(stock_code, created_at DESC);

-- 文档分块表（供 RAG 检索用，已下载的文档切块向量化后存这里）
-- 注意：embedding 列需要 pgvector 扩展，首次初始化时自动尝试添加
CREATE TABLE IF NOT EXISTS biz.doc_chunk (
    id              BIGSERIAL PRIMARY KEY,
    doc_id          BIGINT NOT NULL REFERENCES biz.doc_source_entry(id) ON DELETE CASCADE,
    stock_code      TEXT NOT NULL,
    chunk_index     INTEGER NOT NULL,          -- 第几个块
    chunk_text      TEXT NOT NULL,             -- 块文本
    chunk_tokens    INTEGER,                   -- token 数
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_doc_chunk_doc ON biz.doc_chunk(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_chunk_code ON biz.doc_chunk(stock_code);

-- 可选：如果安装了 pgvector，添加 embedding 列
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'biz' AND table_name = 'doc_chunk' AND column_name = 'embedding'
        ) THEN
            ALTER TABLE biz.doc_chunk ADD COLUMN embedding vector(1536);
        END IF;
    END IF;
END $$;

-- 缺失补全任务队列（按笔记本触发的补齐任务）
CREATE TABLE IF NOT EXISTS biz.notebook_fill_task (
    id              BIGSERIAL PRIMARY KEY,
    stock_code      TEXT NOT NULL,
    fill_type       TEXT NOT NULL,             -- 补齐类型：annual_report / research / finance ...
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending / running / success / failed
    error_msg       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fill_task ON biz.notebook_fill_task(stock_code, status);

-- 自选股（观察列表）
CREATE TABLE IF NOT EXISTS biz.watchlist (
    id              BIGSERIAL PRIMARY KEY,
    stock_code      TEXT NOT NULL UNIQUE,
    stock_name      TEXT,
    note            TEXT,                       -- 备注
    tags            TEXT[] DEFAULT ARRAY[]::TEXT[],  -- 标签
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watchlist_added ON biz.watchlist(added_at DESC);

-- ============================================================
-- 6. 初始化来源平台
-- ============================================================
INSERT INTO sys.source_platform (platform_code, platform_name, base_url, is_free, remark) VALUES
    ('akshare', 'AkShare 开源财经数据接口', 'https://akshare.akfamily.xyz/', TRUE, 'Python 库，聚合东方财富/新浪/巨潮等'),
    ('cninfo', '巨潮资讯网', 'http://www.cninfo.com.cn/', TRUE, '证监会指定信息披露平台'),
    ('eastmoney', '东方财富网', 'https://www.eastmoney.com/', TRUE, '行情 / F10 / 研报 / 资金流向')
ON CONFLICT DO NOTHING;
