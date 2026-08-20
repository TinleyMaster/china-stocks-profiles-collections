# ================== 构建阶段 ==================
FROM python:3.11-slim AS builder

WORKDIR /app

# 安装编译依赖（psycopg2 需要）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 先装依赖，利用 Docker 缓存
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ================== 运行阶段 ==================
FROM python:3.11-slim

WORKDIR /app

# 运行时依赖：libpq 是 psycopg2 需要的
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 从构建阶段拷贝已安装的包
COPY --from=builder /install /usr/local

# 拷贝项目代码
COPY . .

# 确保 logs / data 目录存在
RUN mkdir -p logs data/docs

# 环境变量默认值（Zeabur 上会被覆盖）
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai
ENV DB_HOST=localhost
ENV DB_PORT=5432
ENV DB_NAME=china_stocks
ENV DB_USER=postgres
ENV DB_PASSWORD=postgres
ENV SCHEDULER_ENABLED=true
ENV TIMEZONE=Asia/Shanghai
ENV MAX_WORKERS=4

# 启动入口：先 init-db 再启动调度器
ENTRYPOINT ["bash", "-c", "python -m china_stocks init-db && python -m china_stocks scheduler"]

# 健康检查（每 5 分钟看一下主进程是否还活着）
HEALTHCHECK --interval=5m --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "from china_stocks.db import get_engine; get_engine().connect().close()" || exit 1
