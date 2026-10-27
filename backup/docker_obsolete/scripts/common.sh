#!/usr/bin/env bash
# ==============================================================================
#  COMMON HELPER MODULE FOR AUTORESEARCHING DEPLOYMENT
#  Chứa các tiện ích dùng chung: logging, kiểm tra môi trường, docker wrapper.
# ==============================================================================

set -euo pipefail

# ── Colors & Styles ───────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# ── Logging Functions ─────────────────────────────────────────────────────────
info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*" >&2; }
fatal()   { error "$*"; exit 1; }

section() {
  echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════════════════════${NC}"
  echo -e "${BOLD}${CYAN}  $*${NC}"
  echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════════${NC}"
}

# ── Project Directory & Docker Command ────────────────────────────────────────
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${DEPLOY_DIR}/../.." && pwd)"

detect_docker_compose() {
  if docker compose version &>/dev/null; then
    echo "docker compose"
  elif command -v docker-compose &>/dev/null; then
    echo "docker-compose"
  else
    echo ""
  fi
}

DC=$(detect_docker_compose)

# ── System Environment Validation ─────────────────────────────────────────────
check_system_environment() {
  section "Kiểm tra môi trường hệ thống (Docker-First)"

  # OS Check
  local os
  os=$(uname -s)
  [[ "$os" != "Linux" ]] && warn "Hệ thống phát hiện: $os (Khuyến nghị: Linux VPS Ubuntu/Debian)"
  success "OS: $os"

  # CPU & RAM Check
  local cpu_cores mem_gb
  cpu_cores=$(nproc 2>/dev/null || echo 1)
  mem_gb=$(awk '/MemTotal/ {printf "%.0f", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo 2)
  info "Tài nguyên máy chủ: ${cpu_cores} CPU Cores | ${mem_gb} GB RAM"
  [[ "$cpu_cores" -lt 2 ]] && warn "Khuyến nghị >= 2 CPU cores (hiện có: ${cpu_cores})"
  [[ "$mem_gb" -lt 4 ]]    && warn "Khuyến nghị >= 4 GB RAM (hiện có: ${mem_gb} GB)"

  # Disk Free Space Check
  local disk_free_gb
  disk_free_gb=$(df -BG "$PROJECT_DIR" 2>/dev/null | awk 'NR==2 {gsub("G",""); print $4}')
  if [[ -n "$disk_free_gb" ]]; then
    info "Dung lượng ổ cứng trống: ${disk_free_gb} GB"
    [[ "$disk_free_gb" -lt 8 ]] && warn "Khuyến nghị >= 8 GB trống (hiện có: ${disk_free_gb} GB)"
    success "Ổ cứng: OK"
  fi

  # Docker Engine Check
  if ! command -v docker &>/dev/null; then
    fatal "Docker chưa được cài đặt. Vui lòng cài Docker trước: curl -fsSL https://get.docker.com | bash"
  fi
  local docker_ver
  docker_ver=$(docker --version 2>/dev/null | grep -oP '[\d.]+' | head -1 || echo "unknown")
  success "Docker Engine: v${docker_ver}"

  # Docker Daemon Check
  if ! docker info &>/dev/null; then
    info "Docker daemon chưa chạy, thử khởi động bằng systemctl..."
    sudo systemctl start docker 2>/dev/null || fatal "Không thể khởi động Docker daemon"
  fi
  success "Docker daemon: Đang hoạt động (Running)"

  # Docker Compose Check
  if [[ -z "$DC" ]]; then
    warn "Chưa tìm thấy Docker Compose. Đang cài đặt docker-compose-plugin..."
    sudo apt-get update -qq && sudo apt-get install -y docker-compose-plugin || fatal "Không thể cài đặt docker compose"
    DC="docker compose"
  fi
  local dc_ver
  dc_ver=$($DC version 2>/dev/null | grep -oP '[\d.]+' | head -1 || echo "unknown")
  success "Docker Compose: v${dc_ver} (lệnh: '${DC}')"
}

# ── Project Files & Env Validation ────────────────────────────────────────────
check_project_files() {
  section "Kiểm tra tệp tin cấu hình dự án"
  cd "$PROJECT_DIR"

  local required_files=(
    "docker-compose-infra.yaml"
    "docker-compose-app.yaml"
    "litellm_config.yaml"
    "Dockerfile"
    "requirements.txt"
  )

  for f in "${required_files[@]}"; do
    [[ -f "$f" ]] && success "  Tệp OK: $f" || fatal "Thiếu tệp bắt buộc: $f"
  done

  # Kiểm tra file .env
  if [[ ! -f ".env" ]]; then
    if [[ -f ".env.example" ]]; then
      warn "Chưa có .env. Đang tự động sao chép từ .env.example..."
      cp .env.example .env
      warn ">>> VUI LÒNG ĐIỀN API KEY VÀO FILE .env TRƯỚC KHI KHỞI CHẠY <<<"
    else
      fatal "Không tìm thấy .env và .env.example."
    fi
  fi
  success "Tệp cấu hình .env: Đã tồn tại"

  # Validate Env Keys
  set -a
  # shellcheck source=/dev/null
  source .env 2>/dev/null || true
  set +a

  local has_llm_key=false
  [[ -n "${OPEN_AI_KEY:-}" ]] && has_llm_key=true && success "  OPEN_AI_KEY: Đã cấu hình"
  [[ -n "${GEMINI_KEY:-}" ]]  && has_llm_key=true && success "  GEMINI_KEY: Đã cấu hình"
  [[ -n "${GROQ_API_KEY:-}" ]] && has_llm_key=true && success "  GROQ_API_KEY: Đã cấu hình"

  if [[ "$has_llm_key" == "false" ]]; then
    warn "CẢNH BÁO: Chưa có LLM API key nào (OPEN_AI_KEY, GEMINI_KEY hoặc GROQ_API_KEY) trong .env"
  fi
}

# ── Execute Commands inside Docker Container ──────────────────────────────────
run_in_docker() {
  cd "$PROJECT_DIR"
  local cmd=("$@")

  # Tự động build image nếu chưa tồn tại
  if ! docker image inspect autoresearching-ai:latest &>/dev/null; then
    info "Image autoresearching-ai:latest chưa tồn tại. Đang tiến hành build..."
    docker build --tag autoresearching-ai:latest --file Dockerfile . || fatal "Build image thất bại!"
  fi

  # Chạy lệnh bên trong container với host network và env-file
  docker run --rm -i \
    --network host \
    --env-file .env \
    -v "${PROJECT_DIR}":/app \
    -v /tmp:/tmp/autoresearching \
    autoresearching-ai:latest "${cmd[@]}"
}
