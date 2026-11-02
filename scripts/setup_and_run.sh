#!/usr/bin/env bash
# ==============================================================================
#  SETUP AND RUN SCRIPT (DOCKER-FIRST)
#  Quy chuẩn theo rules/rule-docker.md
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# 1. Kiểm tra Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker chưa được cài đặt." && exit 1
fi
if ! docker info &> /dev/null; then
    echo "❌ Docker daemon chưa khởi động." && exit 1
fi

# 2. Chuẩn bị .env
if [ ! -f .env ]; then
    echo "⚠️  Sao chép .env.example → .env..."
    cp .env.example .env
fi

# 3. Khởi động qua deploy.sh (chuẩn: build image + up docker-compose.yaml + health)
if [[ $# -eq 0 ]]; then
    exec ./deploy.sh all
else
    exec ./deploy.sh "$@"
fi
