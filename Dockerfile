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

# 运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 从构建阶段拷贝已安装的包
COPY --from=builder /install /usr/local

# 拷贝项目代码
COPY . .

# 确保 logs / data 目录存在
RUN mkdir -p logs data/docs

# 环境变量默认值（Zeabur 上会被覆盖）
ENV PYTHONUNBUFFERED=1
ENV DB_HOST=localhost
ENV DB_PORT=5432
ENV DB_NAME=china_stocks
ENV DB_USER=postgres
ENV DB_PASSWORD=postgres
ENV SCHEDULER_ENABLED=true
ENV TIMEZONE=Asia/Shanghai
ENV MAX_WORKERS=4

# 启动脚本
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["scheduler"]

# 健康检查：HTTP 探活（Zeabur 兼容）
HEALTHCHECK --interval=2m --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# 暴露端口
EXPOSE 8080
