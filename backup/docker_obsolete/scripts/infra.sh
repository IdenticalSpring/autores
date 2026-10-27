#!/usr/bin/env bash
# ==============================================================================
#  INFRASTRUCTURE LIFECYCLE & SEEDING MODULE (DOCKER-FIRST)
#  Quản lý hạ tầng (Kafka, Redis, Mongo, Qdrant, MinIO, LiteLLM) và Seed DB.
# ==============================================================================

set -euo pipefail

# ── Start Infrastructure ──────────────────────────────────────────────────────
start_infra() {
  section "Khởi động Hạ tầng (Infrastructure)"
  cd "$PROJECT_DIR"

  info "Tải các Docker image mới nhất cho hạ tầng..."
  $DC -f docker-compose-infra.yaml pull || warn "Một số image pull thất bại, dùng image local có sẵn..."

  info "Khởi động các container hạ tầng trong chế độ chạy ngầm (detached)..."
  $DC -f docker-compose-infra.yaml up -d

  wait_infra_healthy
}

# ── Wait for Containers Healthy ───────────────────────────────────────────────
wait_single_healthy() {
  local container="$1"
  local max_wait="${2:-120}"
  local elapsed=0

  while [[ "$elapsed" -lt "$max_wait" ]]; do
    local status
    status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "not_found")
    case "$status" in
      healthy)
        success "  $container: Hoạt động tốt (healthy)"
        return 0
        ;;
      not_found)
        warn "  $container: Chưa tìm thấy container"
        return 1
        ;;
    esac
    sleep 4
    elapsed=$((elapsed + 4))
    info "  Đợi $container... (${elapsed}s / ${max_wait}s) [trạng thái: $status]"
  done

  warn "  $container: Hết thời gian chờ (${max_wait}s)"
  return 1
}

wait_infra_healthy() {
  section "Kiểm tra trạng thái sẵn sàng của Hạ tầng"
  wait_single_healthy "autoresearching-kafka"   120 || true
  wait_single_healthy "autoresearching-redis"   45  || true
  wait_single_healthy "autoresearching-mongodb" 45  || true
  wait_single_healthy "autoresearching-qdrant"  45  || true
  wait_single_healthy "autoresearching-minio"   45  || true
  wait_single_healthy "autoresearching-litellm" 90  || true
  success "Các dịch vụ hạ tầng đã sẵn sàng phục vụ!"
}

# ── Seed Infrastructure via Docker Container ──────────────────────────────────
seed_infrastructure() {
  section "Khởi tạo dữ liệu hạ tầng (Seed via Docker)"
  cd "$PROJECT_DIR"

  if [[ ! -f "scripts/seed_infra.py" ]]; then
    warn "Không tìm thấy file scripts/seed_infra.py, bỏ qua bước seed."
    return 0
  fi

  info "Thực thi seed_infra.py hoàn toàn bên trong Docker container..."
  info "(Tạo topic Kafka, khởi tạo S3 Bucket MinIO, nạp Redis AI_KEY)"

  if run_in_docker python scripts/seed_infra.py; then
    success "Seed hạ tầng thành công qua Docker!"
  else
    error "Seed hạ tầng gặp lỗi! Hãy kiểm tra log phía trên."
    return 1
  fi
}

# ── Stop & Restart Infrastructure ─────────────────────────────────────────────
stop_infra() {
  section "Dừng toàn bộ dịch vụ Hạ tầng"
  cd "$PROJECT_DIR"
  $DC -f docker-compose-infra.yaml down
  success "Đã dừng các dịch vụ hạ tầng."
}

restart_infra() {
  section "Khởi động lại các dịch vụ Hạ tầng"
  cd "$PROJECT_DIR"
  $DC -f docker-compose-infra.yaml restart
  wait_infra_healthy
}
