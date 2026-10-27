#!/usr/bin/env bash
# ==============================================================================
#  MANAGEMENT & INTERACTIVE SUB-MENUS MODULE
#  Cung cấp các menu con: Xem Logs, Restart dịch vụ, và chạy Test qua Docker.
# ==============================================================================

set -euo pipefail

# ── Interactive Logs Menu ─────────────────────────────────────────────────────
menu_view_logs() {
  echo ""
  echo -e "${CYAN}${BOLD}── XEM LOGS DỊCH VỤ (Nhấn Ctrl+C để thoát xem log) ───────${NC}"
  echo -e "  [1] Tất cả 4 AI Services (Doc, Write, Chat, Enhance)"
  echo -e "  [2] Service DOC        (autoresearching-doc)"
  echo -e "  [3] Service WRITE      (autoresearching-write)"
  echo -e "  [4] Service CHAT       (autoresearching-chat)"
  echo -e "  [5] Service ENHANCE    (autoresearching-enhance)"
  echo -e "  [6] LiteLLM Proxy      (autoresearching-litellm)"
  echo -e "  [7] Kafka Broker       (autoresearching-kafka)"
  echo -e "  [8] Redis Stack        (autoresearching-redis)"
  echo -e "  [9] MongoDB            (autoresearching-mongodb)"
  echo -e "  [10] Qdrant Vector DB  (autoresearching-qdrant)"
  echo -e "  [11] MinIO Storage     (autoresearching-minio)"
  echo -e "  [12] Model Manager UI  (autoresearching-model-manager)"
  echo -e "  [0] Quay lại Menu chính"
  echo ""
  read -rp "Chọn dịch vụ muốn xem logs [0-12]: " log_choice

  case "$log_choice" in
    1) $DC -f docker-compose-app.yaml logs -f --tail 100 || true ;;
    2) docker logs -f --tail 100 autoresearching-doc || true ;;
    3) docker logs -f --tail 100 autoresearching-write || true ;;
    4) docker logs -f --tail 100 autoresearching-chat || true ;;
    5) docker logs -f --tail 100 autoresearching-enhance || true ;;
    6) docker logs -f --tail 100 autoresearching-litellm || true ;;
    7) docker logs -f --tail 100 autoresearching-kafka || true ;;
    8) docker logs -f --tail 100 autoresearching-redis || true ;;
    9) docker logs -f --tail 100 autoresearching-mongodb || true ;;
    10) docker logs -f --tail 100 autoresearching-qdrant || true ;;
    11) docker logs -f --tail 100 autoresearching-minio || true ;;
    12) docker logs -f --tail 100 autoresearching-model-manager || true ;;
    0) return 0 ;;
    *) warn "Lựa chọn không hợp lệ." ;;
  esac
}

# ── Interactive Restart Menu ──────────────────────────────────────────────────
menu_restart_services() {
  echo ""
  echo -e "${YELLOW}${BOLD}── KHỞI ĐỘNG LẠI DỊCH VỤ (RESTART) ───────────────────────${NC}"
  echo -e "  [1] Restart Toàn bộ Hệ thống (Hạ tầng + 4 AI Services)"
  echo -e "  [2] Restart 4 AI Services (Doc, Write, Chat, Enhance)"
  echo -e "  [3] Restart Toàn bộ Hạ tầng (Kafka, Redis, Mongo, Qdrant, MinIO, LiteLLM)"
  echo -e "  [4] Restart riêng một Container cụ thể"
  echo -e "  [0] Quay lại Menu chính"
  echo ""
  read -rp "Chọn phương thức restart [0-4]: " restart_choice

  case "$restart_choice" in
    1)
      info "Restart toàn bộ hạ tầng..."
      $DC -f docker-compose-infra.yaml restart
      info "Restart 4 AI Services..."
      $DC -f docker-compose-app.yaml restart
      success "Đã khởi động lại toàn bộ hệ thống!"
      ;;
    2) restart_app ;;
    3) restart_infra ;;
    4)
      echo ""
      read -rp "Nhập chính xác tên container (vd: autoresearching-doc): " c_name
      if [[ -n "$c_name" ]]; then
        info "Đang restart container: $c_name..."
        docker restart "$c_name" && success "Restart $c_name thành công!" || error "Lỗi restart $c_name"
      fi
      ;;
    0) return 0 ;;
    *) warn "Lựa chọn không hợp lệ." ;;
  esac
}

# ── Interactive Tests Menu (Docker-First) ─────────────────────────────────────
menu_kafka_tests() {
  echo ""
  echo -e "${MAGENTA}${BOLD}── CHẠY TEST TÍCH HỢP KAFKA (Qua Docker Container) ───────${NC}"
  echo -e "  [1] Test Tìm kiếm bài báo OpenAlex (test_kafka_search_papers.py)"
  echo -e "  [2] Test Sinh ý tưởng & Đề cương   (test_kafka_idea_gen.py)"
  echo -e "  [3] Test AI Chatbot Outer/Inner    (test_kafka_chatbot.py)"
  echo -e "  [4] Test Viết báo cáo & Slide       (test_kafka_write.py)"
  echo -e "  [5] Test AI gợi ý cải thiện văn bản (test_kafka_enhance.py)"
  echo -e "  [0] Quay lại Menu chính"
  echo ""
  read -rp "Chọn bài kiểm thử muốn thực thi [0-5]: " test_choice

  case "$test_choice" in
    1) run_in_docker python scripts/test_kafka_search_papers.py ;;
    2) run_in_docker python scripts/test_kafka_idea_gen.py ;;
    3) run_in_docker python scripts/test_kafka_chatbot.py ;;
    4) run_in_docker python scripts/test_kafka_write.py ;;
    5) run_in_docker python scripts/test_kafka_enhance.py ;;
    0) return 0 ;;
    *) warn "Lựa chọn không hợp lệ." ;;
  esac
}
