#!/usr/bin/env bash
# ==============================================================================
#  SETUP AND RUN SCRIPT (DOCKER-FIRST)
#  Quy chuẩn theo rules/rule-docker.md
#  Mọi thao tác khởi động đi qua deploy.sh (chuẩn 1 container).
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

AI_CONTAINER="autoresearching-ai-dev"

# 1. Kiểm tra Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker chưa được cài đặt."
    exit 1
fi
if ! docker info &> /dev/null; then
    echo "❌ Docker daemon chưa khởi động."
    exit 1
fi

# 1b. Kiểm tra deploy.sh tồn tại & thực thi được (trước khi exec)
if [[ ! -x ./deploy.sh ]]; then
    echo "❌ deploy.sh chưa tồn tại hoặc chưa +x. Chạy: chmod +x deploy.sh"
    exit 1
fi

# 2. Chuẩn bị .env
if [ ! -f .env ]; then
    echo "⚠️  Sao chép .env.example → .env..."
    cp .env.example .env
fi

# 3. Chọn chế độ khởi động
#    - Không tham số + CHƯA chạy → "all" (full: build + up + seed + health)
#    - Không tham số + ĐÃ chạy   → "up"  (nhanh: không build lại image)
#    - --force-all               → luôn "all"
#    - tham số khác              → truyền thẳng vào deploy.sh
if [[ $# -eq 0 ]]; then
    if docker ps --format '{{.Names}}' | grep -qx "$AI_CONTAINER"; then
        echo "ℹ️  Hệ thống đang chạy ($AI_CONTAINER) → chế độ NHANH (up, không build lại image)."
        echo "    Muốn build lại image + seed keys + health: ./scripts/setup_and_run.sh --force-all"
        exec ./deploy.sh up
    else
        echo "ℹ️  Hệ thống chưa chạy → Full Deploy (build image + up + seed + health)."
        echo "    Note: bước build image có thể mất vài phút nếu code mới đổi."
        exec ./deploy.sh all
    fi
else
    case "$1" in
        --force-all)
            exec ./deploy.sh all
            ;;
        -h|--help)
            echo "Cách dùng: ./scripts/setup_and_run.sh [--force-all | <cmd>]"
            echo "  (không tham số) : auto — nhanh nếu đã chạy, full nếu chưa"
            echo "  --force-all     : luôn Full Deploy (build + up + seed + health)"
            echo "  <cmd> khác     : truyền thẳng vào ./deploy.sh <cmd> (vd: up, seed, health)"
            exit 0
            ;;
        *)
            exec ./deploy.sh "$@"
            ;;
    esac
fi
