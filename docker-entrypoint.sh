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

# 1. 等待数据库就绪（最多等 120 秒）
echo "→ 等待数据库连接..."
CONNECTED=0
for i in $(seq 1 60); do
    if python -c "
import sys
from sqlalchemy import create_engine
from china_stocks.config import db_url
try:
    engine = create_engine(db_url())
    engine.connect().close()
    print('  ✓ 数据库连接成功')
    sys.exit(0)
except Exception as e:
    print(f'  等待中... ({i}/60)  错误: {e}')
    sys.exit(1)
" 2>&1; then
        CONNECTED=1
        break
    fi
    sleep 2
done

if [ "$CONNECTED" -ne 1 ]; then
    echo "✗ 数据库连接失败，退出。"
    echo "  请检查 DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD 或 DATABASE_URL 配置"
    exit 1
fi

# 2. 初始化数据库
echo ""
echo "→ 初始化数据库..."
if python -m china_stocks init-db 2>&1; then
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
