#!/usr/bin/env bash
# ==============================================================================
#  AUTORESEARCHING — DEPLOY CHUẨN (theo docker-compose.yaml gốc)
#  1 container "autoresearching-ai-dev" chạy main.py (4 listener: doc/write/chat/enhance)
#  Hạ tầng (Kafka/Redis/Mongo/Qdrant/MinIO) phải chạy trước — xem README.md
# ==============================================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_COMPOSE="docker-compose.yaml"
INFRA_COMPOSE="docker-compose-infra.yaml"
IMAGE="autoresearching-ai-dev:latest"
CONTAINER="autoresearching-ai-dev"
cd "$PROJECT_DIR"

usage() {
  cat <<'EOF'
Cách dùng: ./deploy.sh <command>

  build        Build image autoresearching-ai-dev:latest từ Dockerfile
  up           Khởi động container app theo docker-compose.yaml
  infra-up     Khởi động hạ tầng local (docker-compose-infra.yaml)
  infra-down   Dừng hạ tầng local (KHÔNG xóa volume)
  restart      Restart container app
  stop         Dừng container app (KHÔNG xóa volume)
  status       Xem container + port của hệ thống
  logs [name]  Xem log realtime (mặc định: autoresearching-ai-dev)
  health       Kiểm tra nhanh: container Up + 4 listener đã khởi động
  all          build + up + health
EOF
}

require_docker() {
  command -v docker >/dev/null 2>&1 || { echo "[ERR] Docker chưa cài đặt."; exit 1; }
  docker info >/dev/null 2>&1 || { echo "[ERR] Docker daemon chưa chạy."; exit 1; }
}

cmd="${1:-help}"
case "$cmd" in
  build)
    require_docker
    docker build -t "$IMAGE" -f Dockerfile .
    ;;
  up)
    require_docker
    docker compose -f "$APP_COMPOSE" up -d
    ;;
  infra-up)
    require_docker
    docker compose -f "$INFRA_COMPOSE" up -d
    ;;
  infra-down)
    docker compose -f "$INFRA_COMPOSE" stop
    ;;
  restart)
    docker compose -f "$APP_COMPOSE" restart
    ;;
  stop)
    docker compose -f "$APP_COMPOSE" stop
    ;;
  status)
    docker ps --filter "name=autoresearching" \
      --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
    ;;
  logs)
    docker logs -f --tail 200 "${2:-$CONTAINER}"
    ;;
  health)
    echo "[1] Container:"
    docker ps -a --filter "name=$CONTAINER" --format "  {{.Names}}  {{.Status}}"
    echo "[2] Số listener đã khởi động (kỳ vọng: 4):"
    docker logs "$CONTAINER" 2>&1 | grep -c "Ingestor loop started" || true
    echo "[3] Port hạ tầng (nếu chạy local):"
    for p in 9092 6379 27017 6333 9000; do
      if timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$p" 2>/dev/null; then
        echo "  port $p: READY"
      else
        echo "  port $p: không phản hồi"
      fi
    done
    ;;
  all)
    require_docker
    docker build -t "$IMAGE" -f Dockerfile .
    docker compose -f "$APP_COMPOSE" up -d
    "$0" health
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "[ERR] Không nhận diện được lệnh: $cmd"
    usage
    exit 1
    ;;
esac
