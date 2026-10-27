#!/usr/bin/env bash
# ==============================================================================
#  APPLICATION LIFECYCLE & HEALTHCHECK MODULE
#  Quản lý 4 AI Services (Doc, Write, Chat, Enhance) và Healthcheck toàn hệ thống.
# ==============================================================================

set -euo pipefail

# ── Build AI Image ────────────────────────────────────────────────────────────
build_ai_image() {
  section "Xây dựng Docker Image autoresearching-ai:latest"
  cd "$PROJECT_DIR"

  info "Bắt đầu build Docker image từ Dockerfile..."
  docker build \
    --tag autoresearching-ai:latest \
    --file Dockerfile \
    . || fatal "Docker build thất bại! Vui lòng kiểm tra Dockerfile và requirements.txt"

  success "Build image autoresearching-ai:latest thành công!"
}

# ── Start / Stop / Restart 4 AI Services ──────────────────────────────────────
start_app() {
  section "Khởi động 4 AI Service"
  cd "$PROJECT_DIR"

  # Đảm bảo image đã có trước khi chạy
  if ! docker image inspect autoresearching-ai:latest &>/dev/null; then
    info "Chưa tìm thấy image, tiến hành build trước khi chạy..."
    build_ai_image
  fi

  info "Khởi động 4 dịch vụ AI (Doc, Write, Chat, Enhance)..."
  $DC -f docker-compose-app.yaml up -d

  success "Cả 4 AI Service đã được khởi chạy thành công!"
}

stop_app() {
  section "Dừng 4 AI Service"
  cd "$PROJECT_DIR"
  $DC -f docker-compose-app.yaml stop
  $DC -f docker-compose-app.yaml rm -f
  success "Đã dừng 4 AI Service."
}

restart_app() {
  section "Khởi động lại 4 AI Service"
  cd "$PROJECT_DIR"
  $DC -f docker-compose-app.yaml restart
  success "Đã khởi động lại 4 AI Service."
}

# ── Health Check Helpers ──────────────────────────────────────────────────────
check_port() {
  local name="$1" host="$2" port="$3"
  local max_attempts=5
  for ((i=1; i<=max_attempts; i++)); do
    if timeout 2 bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null; then
      success "  ${name}: ${host}:${port} — SẴN SÀNG"
      return 0
    fi
    [[ $i -lt $max_attempts ]] && sleep 1
  done
  warn    "  ${name}: ${host}:${port} — KHÔNG PHẢN HỒI"
}

health_check_all() {
  section "Tổng kiểm tra trạng thái Hệ thống & Cổng mạng"

  echo -e "\n${BOLD}  [1] Danh sách Containers đang chạy:${NC}"
  docker ps --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" \
    | grep -E "autoresearching" | sort || warn "Không tìm thấy container autoresearching nào đang chạy."

  echo -e "\n${BOLD}  [2] Kiểm tra kết nối cổng mạng nội bộ (Localhost):${NC}"
  check_port "Kafka Broker"   "localhost" "9092"
  check_port "Kafka UI"       "localhost" "8080"
  check_port "Redis Stack"    "localhost" "6379"
  check_port "MongoDB"        "localhost" "27017"
  check_port "Qdrant Vector"  "localhost" "6333"
  check_port "MinIO API"      "localhost" "9000"
  check_port "MinIO Console"  "localhost" "9001"
  check_port "LiteLLM Proxy"  "localhost" "8000"
  check_port "Model Manager"  "localhost" "8088"

  echo -e "\n${BOLD}  [3] Kiểm tra LiteLLM API Endpoint:${NC}"
  if curl -sf "http://localhost:8000/health/liveliness" &>/dev/null; then
    success "LiteLLM API: ALIVE (Sẵn sàng phục vụ AI model)"
  else
    warn "LiteLLM API: Chưa phản hồi (vẫn đang khởi động, vui lòng thử lại sau)"
  fi
}

print_summary_banner() {
  echo ""
  echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════╗${NC}"
  echo -e "${GREEN}${BOLD}║        AUTORESEARCHING — HỆ THỐNG ĐÃ SẴN SÀNG            ║${NC}"
  echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════╝${NC}"
  echo ""
  echo -e "  ${BOLD}🌐 Giao diện Quản trị & Dashboard:${NC}"
  echo -e "    - Model Manager:  http://localhost:8088  (Web Quản lý Model AI)"
  echo -e "    - Kafka UI:       http://localhost:8080"
  echo -e "    - MinIO Console:  http://localhost:9001  (minioadmin / minioadmin_local)"
  echo -e "    - Qdrant UI:      http://localhost:6333/dashboard"
  echo -e "    - LiteLLM Proxy:  http://localhost:8000"
  echo ""
  echo -e "  ${BOLD}🤖 Các Dịch vụ AI & Ứng dụng:${NC}"
  echo -e "    - autoresearching-model-manager (Web Quản lý Model :8088)"
  echo -e "    - autoresearching-doc           (Document Setup: Đề cương, Tìm paper)"
  echo -e "    - autoresearching-write         (Write Reports: Viết báo cáo, Slide)"
  echo -e "    - autoresearching-chat          (AI Chatbot: Inner/Outer/Writer bot)"
  echo -e "    - autoresearching-enhance       (Enhancement: AI gợi ý văn bản)"
  echo ""
}
