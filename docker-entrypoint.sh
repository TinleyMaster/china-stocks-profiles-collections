#!/bin/bash
set -e

echo "========================================="
echo " A股投研采集系统 - 启动中"
echo "========================================="
echo " DB_HOST: $DB_HOST"
echo " DB_NAME: $DB_NAME"
echo " DB_USER: $DB_USER"
echo " SCHEDULER_ENABLED: $SCHEDULER_ENABLED"
echo ""

# 1. 等待数据库就绪（最多等 60 秒）
echo "→ 等待数据库连接..."
for i in $(seq 1 30); do
    if python -c "
from sqlalchemy import create_engine
from china_stocks.config import db_url
engine = create_engine(db_url())
engine.connect().close()
" 2>/dev/null; then
        echo "  ✓ 数据库连接成功"
        break
    fi
    echo "  等待中... ($i/30)"
    sleep 2
done

# 2. 初始化数据库
echo ""
echo "→ 初始化数据库..."
if python -m china_stocks init-db; then
    echo "  ✓ 数据库初始化完成"
else
    echo "  ⚠ 数据库初始化失败（可能已存在），继续启动..."
fi

# 3. 执行命令
echo ""
echo "→ 启动: $1"
echo ""

case "$1" in
    scheduler)
        exec python -m china_stocks scheduler
        ;;
    shell)
        exec bash
        ;;
    *)
        exec python -m china_stocks "$@"
        ;;
esac
