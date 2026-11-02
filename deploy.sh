#!/usr/bin/env bash
# ==============================================================================
#  AUTORESEARCHING — QUẢN TRỊ & TRIỂN KHAI DOCKER (CHUẨN 1 CONTAINER)
#  1 container "autoresearching-ai-dev" chạy main.py (4 listener chung 1 process)
# ==============================================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_COMPOSE="docker-compose.yaml"
INFRA_COMPOSE="docker-compose-infra.yaml"
IMAGE="autoresearching-ai-dev:latest"
CONTAINER="autoresearching-ai-dev"
cd "$PROJECT_DIR"

# Màu sắc hiển thị
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

require_docker() {
  command -v docker >/dev/null 2>&1 || { echo -e "${RED}[ERR] Docker chưa được cài đặt.${NC}"; exit 1; }
  docker info >/dev/null 2>&1 || { echo -e "${RED}[ERR] Docker daemon chưa chạy.${NC}"; exit 1; }
}

build_image() {
  require_docker
  echo -e "\n${CYAN}🐳 Đang build Docker Image: ${BOLD}${IMAGE}${NC}..."
  docker build -t "$IMAGE" -f Dockerfile .
  echo -e "${GREEN}✅ Build thành công image ${IMAGE}!${NC}"
}

start_infra() {
  require_docker
  echo -e "\n${CYAN}🏗️  Đang khởi động cụm hạ tầng local (${INFRA_COMPOSE})...${NC}"
  docker compose -f "$INFRA_COMPOSE" up -d
  echo -e "${GREEN}✅ Cụm hạ tầng đã sẵn sàng!${NC}"
}

start_app() {
  require_docker
  echo -e "\n${CYAN}🤖 Đang khởi động AI Container (${APP_COMPOSE})...${NC}"
  docker compose -f "$APP_COMPOSE" up -d
  echo -e "${GREEN}✅ AI Container [${CONTAINER}] đã khởi động!${NC}"
}

# ==============================================================================
#  CHỜ HẠ TẦNG — kiểm tra port theo thời gian thực (không làm giả kết quả)
# ==============================================================================
wait_port() {
  # wait_port <port> <tên dịch vụ> <giây chờ tối đa (mặc định 60)>
  local port="$1" name="$2" timeout="${3:-60}"
  local i=0
  while ! timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; do
    if (( i >= timeout )); then
      echo -e "${YELLOW}⚠️  Chờ port $port ($name) hết $timeout giây, bỏ qua bước chờ.${NC}"
      return 1
    fi
    sleep 1
    i=$((i + 1))
  done
  echo -e "  ✓ Port $port ($name): READY"
}

wait_infra() {
  echo -e "\n${CYAN}⏳ Đang chờ cụm hạ tầng sẵn sàng (Kafka, Redis, Mongo, Qdrant, MinIO, LiteLLM)...${NC}"
  # Dịch vụ nào chưa kịp lên chỉ bị cảnh báo (|| true), không phá pipeline dưới set -e
  wait_port 9092  Kafka   || true
  wait_port 6379  Redis   || true
  wait_port 27017 Mongo   || true
  wait_port 6333  Qdrant  || true
  wait_port 9000  MinIO   || true
  wait_port 8000  LiteLLM || true
}

# ==============================================================================
#  SEED — chạy script seed CÓ SẴN trong image (docker exec, cần container đang chạy)
# ==============================================================================
check_app_running() {
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo -e "${YELLOW}⚠️  Container [${CONTAINER}] chưa chạy — khởi động trước (mục [4]).${NC}"
    return 1
  fi
}

seed_keys() {
  # Đồng bộ API keys từ .env vào Redis + tạo MinIO buckets + kiểm tra Mongo/Qdrant
  require_docker
  echo -e "\n${CYAN}🌱 Đang seed hạ tầng (scripts/seed_infra.py): đồng bộ API Keys .env → Redis...${NC}"
  check_app_running || return 0
  if docker exec "$CONTAINER" python scripts/seed_infra.py; then
    echo -e "${GREEN}✅ Seed hạ tầng thành công!${NC}"
  else
    echo -e "${RED}❌ Seed thất bại — xem output phía trên (Redis 6379 / .env trong container).${NC}"
  fi
}

_run_fixture_seed() {
  # _run_fixture_seed <script> <nhan label> [tham số thêm...]
  local script="$1" label="$2"; shift 2
  require_docker
  echo -e "\n${CYAN}📦 Đang seed [${label}] (python scripts/${script} $*)...${NC}"
  check_app_running || return 0
  if docker exec "$CONTAINER" python "scripts/${script}" "$@"; then
    echo -e "${GREEN}✅ [${label}] seed xong!${NC}"
  else
    echo -e "${RED}❌ [${label}] seed LỖI — xem output phía trên.${NC}"
  fi
}

seed_fixtures() {
  # Submenu seed dữ liệu test (fixture) cho pipeline — chạy trong container
  local choice
  echo -e "\n${CYAN}Chọn bộ fixture (chạy trong container, cần hạ tầng + LiteLLM local):${NC}"
  echo -e "  ${BOLD}[1]${NC} 📄 Outline refs fixture (smoke-outline-refs-1)"
  echo -e "  ${BOLD}[2]${NC} 📦 P2 fixtures (CSV khảo sát + Mongo + Qdrant)"
  echo -e "  ${BOLD}[3]${NC} 🧹 Dọn P2 fixtures (--cleanup)"
  echo -e "  ${BOLD}[0]${NC} Quay lại menu chính"
  read -rp "Chọn [0-3]: " choice
  case "$choice" in
    1) _run_fixture_seed "seed_outline_refs_fixture.py" "Outline refs" ;;
    2) _run_fixture_seed "seed_p2_fixtures.py" "P2 fixtures" ;;
    3) _run_fixture_seed "seed_p2_fixtures.py" "P2 cleanup" --cleanup ;;
    0) return 0 ;;
    *) echo -e "${RED}Lựa chọn không hợp lệ.${NC}" ;;
  esac
}

# ==============================================================================
#  STATS — mức tiêu thụ RAM/CPU (so với mem_limit trong compose)
# ==============================================================================
show_stats() {
  require_docker
  echo -e "\n${CYAN}${BOLD}📈 MỨC TIÊU THỤ TÀI NGUYÊN HIỆN TẠI (RAM / CPU)${NC}"
  echo "------------------------------------------------------------"
  docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
  echo -e "${YELLOW}So sánh với mem_limit trong docker-compose.yaml (APP_MEM_LIMIT).${NC}"
  echo "------------------------------------------------------------"
}

full_deploy() {
  start_infra
  build_image
  start_app
  # Chờ Redis lên trước khi seed keys (seed cần kết nối được Redis 6379)
  wait_port 6379 "Redis" 90 || true
  seed_keys
  health_check
}

restart_app() {
  require_docker
  echo -e "\n${CYAN}🔄 Đang khởi động lại AI Container...${NC}"
  docker compose -f "$APP_COMPOSE" restart
  echo -e "${GREEN}✅ Đã khởi động lại thành công!${NC}"
}

stop_app() {
  echo -e "\n${YELLOW}🛑 Đang dừng AI Container (giữ nguyên volume)...${NC}"
  docker compose -f "$APP_COMPOSE" stop
  echo -e "${GREEN}✅ Đã dừng AI Container!${NC}"
}

stop_all() {
  echo -e "\n${YELLOW}🛑 Đang dừng AI Container và Cụm Hạ tầng...${NC}"
  docker compose -f "$APP_COMPOSE" stop 2>/dev/null || true
  docker compose -f "$INFRA_COMPOSE" stop 2>/dev/null || true
  echo -e "${GREEN}✅ Đã dừng toàn bộ hệ thống!${NC}"
}

health_check() {
  echo -e "\n${CYAN}${BOLD}📊 KIỂM TRA TRẠNG THÁI HỆ THỐNG${NC}"
  echo "------------------------------------------------------------"
  echo -e "${BOLD}[1] Container AI:${NC}"
  docker ps -a --filter "name=$CONTAINER" --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}"
  echo -e "\n${BOLD}[2] Số listener đã nạp trong container (kỳ vọng: 4):${NC}"
  local count
  count=$(docker logs "$CONTAINER" 2>&1 | grep -c "Listener concurrency" || true)
  if [[ "$count" -ge 4 ]]; then
    echo -e "  ${GREEN}✅ Đầy đủ $count/4 listeners đang chạy.${NC}"
  else
    echo -e "  ${YELLOW}⚠️  Hiện có $count listeners (đang khởi tạo hoặc kiểm tra logs).${NC}"
  fi
  echo -e "\n${BOLD}[3] Trạng thái cổng dịch vụ hạ tầng:${NC}"
  for p in 9092:Kafka 6379:Redis 27017:Mongo 6333:Qdrant 9000:MinIO; do
    port="${p%%:*}"
    name="${p##*:}"
    if timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      echo -e "  - Port $port ($name): ${GREEN}READY${NC}"
    else
      echo -e "  - Port $port ($name): ${YELLOW}Chưa kết nối / Remote${NC}"
    fi
  done
  echo "------------------------------------------------------------"
}

view_logs() {
  echo -e "\n${CYAN}📜 Đang mở logs thời gian thực của container [${CONTAINER}] (Ctrl+C để thoát)...${NC}\n"
  docker logs -f --tail 150 "$CONTAINER"
}

main_menu() {
  while true; do
    echo ""
    echo -e "${CYAN}${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║    AUTORESEARCHING — QUẢN TRỊ TRIỂN KHAI (1 CONTAINER)     ║${NC}"
    echo -e "${CYAN}${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo -e "  ${BOLD}[1]${NC} 🚀 ${GREEN}Full Deploy${NC} (Infra + Build + Start + Seed Keys)"
    echo -e "  ${BOLD}[2]${NC} 🏗️  Khởi động Cụm Hạ tầng Local (Kafka, Redis, Mongo, MinIO)"
    echo -e "  ${BOLD}[3]${NC} 🐳 Build Docker Image (${IMAGE})"
    echo -e "  ${BOLD}[4]${NC} 🤖 Khởi động AI Container (Chạy 4 listeners chung 1 process)"
    echo -e "  ${BOLD}[5]${NC} 📊 Kiểm tra Trạng thái & Cổng dịch vụ (Health Check)"
    echo -e "  ${BOLD}[6]${NC} 📜 Xem Logs thời gian thực (Realtime Logs)"
    echo -e "  ${BOLD}[7]${NC} 🔄 Khởi động lại AI Container (Restart)"
    echo -e "  ${BOLD}[8]${NC} ⏸️  Dừng AI Container"
    echo -e "  ${BOLD}[9]${NC} 🛑 Dừng toàn bộ (AI Container + Hạ tầng)"
    echo -e "  ${BOLD}[10]${NC} 🌱 ${YELLOW}Seed hạ tầng (đồng bộ API Keys .env → Redis, MinIO buckets)${NC}"
    echo -e "  ${BOLD}[11]${NC} 📦 Seed dữ liệu test / fixtures (chọn bộ)"
    echo -e "  ${BOLD}[12]${NC} 📈 Xem Mức Tiêu Thụ RAM / CPU (Resource Stats)"
    echo -e "  ${BOLD}[0]${NC} 🚪 Thoát"
    echo -e "${CYAN}────────────────────────────────────────────────────────────${NC}"
    read -rp "👉 Chọn tác vụ [0-12]: " choice

    case "$choice" in
      1) full_deploy ;;
      2) start_infra ;;
      3) build_image ;;
      4) start_app ;;
      5) health_check ;;
      6) view_logs ;;
      7) restart_app ;;
      8) stop_app ;;
      9) stop_all ;;
      10) seed_keys ;;
      11) seed_fixtures ;;
      12) show_stats ;;
      0) echo -e "\n${GREEN}Tạm biệt! Dịch vụ vẫn tiếp tục chạy trên Docker.${NC}\n"; exit 0 ;;
      *) echo -e "${RED}Lựa chọn không hợp lệ. Vui lòng chọn lại!${NC}" ;;
    esac
    echo ""
    read -rp "Nhấn [Enter] để quay lại Menu..." _
  done
}

# Điều hướng: nếu không truyền tham số thì mở Menu, nếu có thì chạy CLI
if [[ $# -eq 0 ]] || [[ "$1" == "--menu" ]] || [[ "$1" == "menu" ]]; then
  main_menu
else
  cmd="$1"
  case "$cmd" in
    build)       build_image ;;
    up)          start_app ;;
    up-wait)     start_app && wait_infra ;;
    infra-up)    start_infra ;;
    infra-down)  docker compose -f "$INFRA_COMPOSE" stop ;;
    restart)     restart_app ;;
    stop)        stop_app ;;
    seed|sync)   seed_keys ;;
    seed-outline) _run_fixture_seed "seed_outline_refs_fixture.py" "Outline refs" ;;
    seed-p2)     _run_fixture_seed "seed_p2_fixtures.py" "P2 fixtures" ;;
    seed-p2-cleanup) _run_fixture_seed "seed_p2_fixtures.py" "P2 cleanup" --cleanup ;;
    status|health) health_check ;;
    stats)       show_stats ;;
    wait-infra)  wait_infra ;;
    logs)        docker logs -f --tail 200 "${2:-$CONTAINER}" ;;
    all)         full_deploy ;;
    help|-h|--help)
      echo "Cách dùng: ./deploy.sh [build|up|up-wait|infra-up|infra-down|restart|stop|seed|seed-outline|seed-p2|seed-p2-cleanup|health|stats|wait-infra|logs|all]"
      echo "  up-wait = khởi động app + chờ hạ tầng sẵn sàng; seed* = seed trong container (cần app đang chạy)"
      echo "  seed = đồng bộ API Keys .env → Redis (seed_infra.py)"
      echo "  seed-outline / seed-p2 / seed-p2-cleanup = seed bộ fixture dữ liệu test"
      ;;
    *)
      echo -e "${RED}[ERR] Lệnh không hợp lệ: $cmd${NC}"
      echo "Chạy ./deploy.sh để mở Menu tương tác số."
      exit 1
      ;;
  esac
fi
