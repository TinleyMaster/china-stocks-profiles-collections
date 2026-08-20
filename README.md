# A 股投研资料采集系统 (China Stocks Research Collection)

一套面向 A 股市场的**投研信息采集与沉淀系统**，参考 [crypto-profile-collection](https://github.com/TinleyMaster/crypto-profile-collection) 的分层架构思想，针对 A 股做定制化适配。

核心思路：把"从多数据源抓取股票资料 → 清洗结构化 → 形成投研资产"这件事，拆成**层层可维护的流水线**，并在末端提供投研分析工具箱。

**零付费数据源起步**：全部基于 akshare（东方财富/新浪/巨潮等免费接口的聚合库），不需要 Wind/Choice 等付费终端。

---

## 技术架构

```
┌─────────────────────────────────────────────────────────────┐
│ biz  业务消费层                                              │
│   stock_basic         — 最新行情 + 估值快照                   │
│   finance_snapshot    — 财务指标画像（ROE/毛利率/增速...）     │
│   capital_snapshot    — 资金面画像（北向/融资融券）            │
│   shareholder_snapshot— 股东快照                              │
│   doc_source_entry    — 文档入口（公告/研报/调研纪要）         │
│   corporate_event     — 公司事件（分红/解禁/回购/业绩预告）     │
├─────────────────────────────────────────────────────────────┤
│ core 统一实体层                                               │
│   stock               — 股票主表（代码唯一）                   │
│   stock_source_map    — 跨数据源映射                           │
├─────────────────────────────────────────────────────────────┤
│ src_akshare 来源解析层                                        │
│   stock_list          — 股票列表快照                           │
│   sw_industry         — 申万行业快照                           │
│   stock_daily         — 日线行情（增量追加）                   │
│   financial_report    — 三大报表（按报告期）                   │
│   financial_indicator — 财务指标（按报告期）                   │
├─────────────────────────────────────────────────────────────┤
│ raw  原始响应层（api_response，用于回溯与重算）                │
├─────────────────────────────────────────────────────────────┤
│ sys  系统元数据层（ingest_run / source_platform）             │
└─────────────────────────────────────────────────────────────┘
```

## 流水线阶段

| 阶段 | 内容 | 频率 | 状态 |
|---|---|---|---|
| Phase A | 股票池构建（全 A 列表 + 申万行业 → core.stock） | 日频 | ✅ 已实现 |
| Phase B1 | 日线行情采集（增量） | 日频（收盘后） | ✅ 已实现 |
| Phase B2 | 公告入口发现 + PDF 下载（巨潮资讯网） | 日频 | ✅ 已实现 |
| Phase B3 | 券商研报 + 调研纪要（东财研报中心） | 日频 | ✅ 已实现 |
| Phase C1 | stock_basic 估值画像（PE/PB/市值...） | 日频 | ✅ 已实现 |
| Phase C2 | finance_snapshot 财务指标画像 | 周频 | ✅ 已实现 |
| Phase C3 | 资金面画像（北向/融资融券） | 日频 | ✅ 已实现 |
| Phase C4 | 股东画像（十大股东/机构持仓/质押/户数） | 周频 | ✅ 已实现 |
| Phase D1 | 公司事件结构化（分红/解禁/业绩预告/回购/增减持） | 周频 | ✅ 已实现 |
| Phase D2 | 投研笔记本（每股 Notebook + 完整性清单 + RAG 问答） | 周频/按需 | ✅ 已实现 |

## 目录结构

```
china-stocks/
├── china_stocks/             # 主包
│   ├── __init__.py
│   ├── __main__.py           # CLI 入口（20+ 命令）
│   ├── config.py             # 配置（.env）
│   ├── logging_setup.py      # 日志
│   ├── scheduler.py          # APScheduler 调度器（13 个定时任务）
│   ├── db/                   # 数据库连接
│   ├── sys/                  # sys 层（采集运行记录）
│   ├── raw/                  # raw 层（原始响应存储）
│   ├── core/                 # core 层（统一实体）
│   ├── mapping/              # 分类体系
│   │   ├── __init__.py       # 公告 taxonomy（22 类 content_topics）
│   │   └── industry.py       # 行业分组映射
│   ├── src/                  # src 层（来源解析）
│   │   ├── akshare_client.py # akshare 统一封装 + 重试
│   │   ├── phase_a_stock_pool.py   # Phase A 股票池
│   │   ├── phase_b_daily.py        # Phase B1 日线行情
│   │   ├── phase_b2_announcements.py # Phase B2 公告入口
│   │   ├── phase_b2_download.py    # Phase B2 文档 PDF 下载（通用）
│   │   ├── phase_b3_research.py    # Phase B3 券商研报
│   │   ├── phase_b3_survey.py      # Phase B3 调研纪要
│   │   └── phase_d_events.py       # Phase D1 公司事件
│   └── biz/                  # biz 层（业务消费）
│       ├── stock_basic.py         # 估值画像
│       ├── finance_snapshot.py    # 财务画像
│       ├── capital_snapshot.py    # 资金面画像
│       ├── shareholder_snapshot.py# 股东画像
│       ├── research_notebook.py   # Phase D2 投研笔记本 + 完整性清单
│       ├── notebook_fill.py       # 缺失资料一键补齐
│       ├── llm_client.py          # LLM 客户端（可插拔 OpenAI 兼容）
│       └── rag.py                 # RAG 问答（检索 + 生成 + 来源标注）
├── db/
│   └── init.sql              # 数据库初始化 DDL（5 schema + 20+ 表）
├── data/docs/                # 下载的文档 PDF（本地，git 忽略）
├── logs/                     # 运行日志（git 忽略）
├── requirements.txt
├── .env.example
└── README.md
```

## 快速上手

### 1. 环境准备

```bash
# Python 3.10+
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. 配置数据库

需要 PostgreSQL 12+（推荐 14+）。

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑 .env，填入你的数据库连接信息
DB_HOST=localhost
DB_PORT=5432
DB_NAME=china_stocks
DB_USER=postgres
DB_PASSWORD=your_password
```

创建数据库：

```sql
CREATE DATABASE china_stocks;
```

### 3. 初始化数据库

```bash
python -m china_stocks init-db
```

会创建 5 个 schema（sys / raw / src_akshare / core / biz）及全部表。

### 4. 跑第一次采集

```bash
# Phase A: 构建股票池（全 A 股列表 + 行业分类 → core.stock）
python -m china_stocks phase-a

# Phase B: 日线行情（增量，首次会自动拉近 2 年历史数据）
python -m china_stocks phase-daily

# 只测几只股票：
python -m china_stocks phase-daily --codes 600519,000001,300750

# Phase C: 估值画像
python -m china_stocks stock-basic

# Phase C: 财务指标
python -m china_stocks finance

# Phase B2: 公告入口（巨潮资讯网，默认增量）
python -m china_stocks announcements

# Phase B3: 券商研报
python -m china_stocks research

# 下载文档 PDF 到本地
python -m china_stocks download-docs --limit 100
```

### 5. 投研笔记本（Phase D）

每股一个 Notebook，自动汇总资料并给出完整度评分。

```bash
# 全量刷新所有股票的笔记本（完整性清单）
python -m china_stocks notebook-build

# 只刷新一只
python -m china_stocks notebook-build --code 600519

# 查看某只股票的笔记本概览 + 完整性清单
python -m china_stocks notebook-info 600519

# 查看缺失资料
python -m china_stocks notebook-missing 600519

# 一键补齐缺失（画像/事件类自动补，文档类提示全量回溯）
python -m china_stocks notebook-fill 600519
```

### 6. RAG 问答

对单只股票基于本地资料库提问，回答自带来源标注。

```bash
# 配置 LLM（.env 中设置）
LLM_PROVIDER=openai_compatible
LLM_API_KEY=sk-xxx
LLM_BASE_URL=https://api.deepseek.com/v1
LLM_MODEL=deepseek-chat

# 提问
python -m china_stocks ask 600519 "最近三年毛利率变化趋势如何？"
```

未配置 LLM 时会返回 Mock 模式提示，不影响其他功能使用。

### 7. 查看采集状态

```bash
python -m china_stocks status
```

### 8. 启动调度器（生产环境）

```bash
python -m china_stocks scheduler
```

默认调度时间（13 个定时任务）：
- 每日 08:30 — Phase A 股票池刷新
- 每日 16:00 — 日线行情
- 每日 17:30 — 估值画像
- 每日 19:00 — 公告入口
- 每日 20:00 — 券商研报
- 每日 20:30 — 调研纪要
- 每日 21:00 — 资金面画像
- 周一 20:00 — 财务指标
- 周三 03:00 — 公司事件
- 周日 03:00 — 股东画像
- 周日 06:00 — 投研笔记本刷新
- 周二/六 02:00 — 文档 PDF 下载

配置了 SMTP 的话，任务失败会自动发邮件告警。

## Docker 部署

### 本地 docker-compose（推荐测试用）

```bash
# 启动（PostgreSQL + 应用）
docker-compose up -d

# 查看日志
docker-compose logs -f app

# 停止
docker-compose down
```

数据持久化：
- PostgreSQL 数据：Docker volume `pg_data`
- 文档 PDF：`./data/`
- 日志：`./logs/`

### Zeabur 一键部署

项目已包含 `zeabur.yaml` 和 `Dockerfile`，支持 Zeabur 直接部署。

步骤：

1. **新建 PostgreSQL 服务**：在 Zeabur 控制台添加 PostgreSQL 服务，记下连接信息
2. **部署应用**：从 GitHub 仓库导入，Zeabur 会自动识别 `Dockerfile`
3. **配置环境变量**：
   - `DB_HOST`：PostgreSQL 服务的内部域名
   - `DB_PORT`：5432
   - `DB_NAME`：数据库名
   - `DB_USER`：用户名
   - `DB_PASSWORD`：密码（设为 Secret）
   - `SCHEDULER_ENABLED`：`true`
   - `TIMEZONE`：`Asia/Shanghai`
   - `MAX_WORKERS`：`4`（根据机器规格调整）
4. **存储卷**：挂两个卷
   - `/app/data`（1GB，存下载的 PDF）
   - `/app/logs`（512MB，存日志）
5. **重启应用**：首次启动会自动执行 `init-db` 初始化表结构

> 注意：akshare 的部分接口需要访问国内网站，建议选择 Zeabur 的国内/香港节点以提高采集速度。

## 设计要点

### 为什么不用 n8n？
参考 crypto 项目的演化路径：n8n 在业务逻辑强耦合的场景下反而增加维护成本。本系统用 APScheduler 内置调度，任务失败自动落库 + 邮件告警，轻量且足够。n8n 可保留给**跨系统集成 / 人工审批流**等真正需要编排的场景。

### 为什么分层？
- `raw` 层存原始响应，方便"数仓重跑"——接口解析错了可以从原始数据重算，不用重新请求
- `src_*` 层按数据源隔离，未来加 Eastmoney 直连、加 Tushare 都是加一个 src 层的事
- `core` 层是唯一真相源——所有上游数据最终都映射到 core.stock，下游只认 stock_code
- `biz` 层面向投研消费，表结构直接对应投研维度

### 为什么选 akshare 而不是付费数据源？
akshare 已经封装了东方财富、新浪、巨潮等绝大多数免费接口，覆盖：
- 行情、资金流、龙虎榜、融资融券
- 三大报表、财务指标
- 公告、研报、调研纪要
- 股东、增减持、分红

对个人投研来说**完全够用**。未来需要更稳定的数据源时，加一个 `src_tushare` 或 `src_wind` 层即可，上层不动。

### A 股 vs 加密货币的核心差异（架构上的体现）

| 维度 | 加密货币 | A 股 |
|---|---|---|
| 实体唯一标识 | 合约地址（跨链多地址，匹配复杂） | 6 位股票代码（天然唯一，Phase A 大幅简化） |
| 文档类型 | 白皮书 / docs / GitHub / 审计 | 财报 / 公告 / 招股书 / 研报 / 调研纪要 |
| 核心投研维度 | tokenomics / unlock / onchain | 财务 / 估值 / 资金面 / 股东 / 行业 / 事件 |
| 行业分类 | 12 类赛道（自定义） | 申万一级/二级/三级（行业标准） |
| 监管与数据规范 | 无序、各自为政 | 证监会强制披露，数据规范、来源统一 |

## 后续扩展建议

优先级从高到低：

1. **Phase B2 公告采集**（巨潮资讯网，最有价值的文本数据）
2. **Phase C3 资金面**（北向 + 融资融券 + 龙虎榜，A 股特有 Alpha 因子）
3. **Phase B3 研报采集**（券商研报，观点提取）
4. **Phase C4 股东画像**（机构持仓变化、质押率）
5. **Phase D 一键投研 RAG**（NotebookLM 风格，每股一个资料库）
6. **Flask Web 工作台**（搜索 + 资料完整性清单 + 投研面板）

## 许可证

MIT
