#!/usr/bin/env bash
# ==============================================================================
#  AUTORESEARCHING — HỆ THỐNG QUẢN TRỊ & TRIỂN KHAI DOCKER-FIRST
#  Hỗ trợ Menu CLI tương tác trực quan hoặc chạy tự động hóa qua CLI flags.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Nạp các module chức năng
# shellcheck source=scripts/deploy/common.sh
source "${SCRIPT_DIR}/scripts/deploy/common.sh"
# shellcheck source=scripts/deploy/infra.sh
source "${SCRIPT_DIR}/scripts/deploy/infra.sh"
# shellcheck source=scripts/deploy/app.sh
source "${SCRIPT_DIR}/scripts/deploy/app.sh"
# shellcheck source=scripts/deploy/manage.sh
source "${SCRIPT_DIR}/scripts/deploy/manage.sh"

# ── Quy trình Full Deploy Toàn bộ Hệ thống ────────────────────────────────────
full_deploy() {
  section "BẮT ĐẦU QUY TRÌNH TRIỂN KHAI TOÀN DIỆN (FULL DEPLOY)"
  check_system_environment
  check_project_files
  start_infra
  build_ai_image
  seed_infrastructure
  start_app
  health_check_all
  print_summary_banner
}

# ── Hướng dẫn sử dụng CLI Flags (Automation / CI-CD) ──────────────────────────
show_help() {
  cat << EOF
Cách dùng: ./deploy.sh [OPTIONS]

Chạy không kèm cờ để mở Giao diện Menu Tương tác (Interactive CLI Menu).

Các tùy chọn dòng lệnh (CLI Flags):
  --all            Triển khai toàn bộ (Kiểm tra + Hạ tầng + Build + Seed + App)
  --infra-only     Chỉ khởi động các dịch vụ Hạ tầng
  --app-only       Chỉ khởi động 4 AI Services (Hạ tầng phải chạy trước)
  --build          Build Docker image autoresearching-ai:latest
  --seed           Chạy seed_infra.py qua Docker container (không cần venv)
  --status         Kiểm tra trạng thái containers và các cổng mạng
  --logs [NAME]    Xem logs thời gian thực (tùy chọn: doc, write, chat, enhance)
  --restart [NAME] Khởi động lại dịch vụ (tùy chọn: app, infra, hoặc tên container)
  --down           Dừng toàn bộ hệ thống (App + Hạ tầng)
  --test           Mở menu kiểm thử Kafka tích hợp qua Docker
  --menu           Mở Menu tương tác (Mặc định khi không có tham số)
  --help, -h       Hiển thị trợ giúp này
EOF
}

# ── Giao diện Menu Tương tác Trực quan ────────────────────────────────────────
main_menu() {
  while true; do
    echo ""
    echo -e "${CYAN}${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║    AUTORESEARCHING — QUẢN TRỊ & TRIỂN KHAI DOCKER-FIRST    ║${NC}"
    echo -e "${CYAN}${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
    echo -e "  ${BOLD}[1]${NC}  🚀 ${GREEN}Full Deploy${NC} (Infra + Build Image + Seed + 4 AI Services)"
    echo -e "  ${BOLD}[2]${NC}  🏗️  Khởi động Hạ tầng (Kafka, Redis, Mongo, Qdrant, MinIO, LiteLLM)"
    echo -e "  ${BOLD}[3]${NC}  🐳 Build Docker Image (autoresearching-ai:latest)"
    echo -e "  ${BOLD}[4]${NC}  🌱 ${YELLOW}Seed Dữ liệu Hạ tầng qua Docker${NC} (AI_KEY, MinIO, Topics)"
    echo -e "  ${BOLD}[5]${NC}  🤖 Khởi động 4 AI Services (Doc, Write, Chat, Enhance)"
    echo -e "  ${BOLD}[6]${NC}  📊 Kiểm tra Trạng thái & Cổng dịch vụ (Health Check)"
    echo -e "  ${BOLD}[7]${NC}  📜 Xem Logs thời gian thực (Interactive Log Viewer)"
    echo -e "  ${BOLD}[8]${NC}  🔄 Khởi động lại Dịch vụ (Restart Menu)"
    echo -e "  ${BOLD}[9]${NC}  🧪 ${MAGENTA}Chạy Test tích hợp Kafka qua Docker${NC} (Không cần venv)"
    echo -e "  ${BOLD}[10]${NC} 🛑 Dừng Hệ thống (Stop / Down)"
    echo -e "  ${BOLD}[0]${NC}  🚪 Thoát"
    echo -e "${CYAN}────────────────────────────────────────────────────────────${NC}"
    read -rp "👉 Chọn tác vụ [0-10]: " choice

    case "$choice" in
      1)  full_deploy ;;
      2)  check_project_files && start_infra ;;
      3)  build_ai_image ;;
      4)  check_project_files && seed_infrastructure ;;
      5)  check_project_files && start_app ;;
      6)  health_check_all ;;
      7)  menu_view_logs ;;
      8)  menu_restart_services ;;
      9)  menu_kafka_tests ;;
      10)
        echo ""
        info "Dừng 4 AI Service..."
        stop_app
        info "Dừng Hạ tầng..."
        stop_infra
        success "Đã dừng toàn bộ dịch vụ của hệ thống!"
        ;;
      0)
        echo -e "\n${GREEN}Tạm biệt! Hệ thống vẫn tiếp tục chạy ổn định trên Docker.${NC}\n"
        exit 0
        ;;
      *)
        warn "Lựa chọn không hợp lệ. Vui lòng chọn lại!"
        ;;
    esac

    echo ""
    read -rp "Nhấn [Enter] để quay lại Menu chính..." _
  done
}

# ── Điều hướng CLI Flags hoặc Interactive Menu ────────────────────────────────
main() {
  if [[ $# -eq 0 ]] || [[ "$1" == "--menu" ]] || [[ "$1" == "menu" ]]; then
    main_menu
    exit 0
  fi

  case "$1" in
    --all)          full_deploy ;;
    --infra-only)   check_project_files && start_infra ;;
    --app-only)     check_project_files && start_app ;;
    --build)        build_ai_image ;;
    --seed)         check_project_files && seed_infrastructure ;;
    --status)       health_check_all ;;
    --test)         menu_kafka_tests ;;
    --down)
      stop_app
      stop_infra
      ;;
    --logs)
      shift
      local target="${1:-}"
      if [[ -z "$target" ]]; then
        menu_view_logs
      else
        docker logs -f --tail 100 "autoresearching-${target}" 2>/dev/null \
          || $DC -f docker-compose-app.yaml logs -f --tail 100 "$target"
      fi
      ;;
    --restart)
      shift
      local target="${1:-}"
      if [[ -z "$target" ]]; then
        menu_restart_services
      elif [[ "$target" == "app" ]]; then
        restart_app
      elif [[ "$target" == "infra" ]]; then
        restart_infra
      else
        docker restart "autoresearching-${target}" 2>/dev/null \
          || docker restart "$target"
      fi
      ;;
    --help|-h)      show_help ;;
    *)
      warn "Không nhận diện được tùy chọn: $1"
      show_help
      exit 1
      ;;
  esac
}

main "$@"
