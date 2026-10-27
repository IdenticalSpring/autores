# 🧹 PLAN CLEANUP & RESET — Đưa Docker về chuẩn gốc, chuyển phần cần thiết từ bản mới

> **Người lập:** Zoo (Senior DevOps/Platform Engineer)
> **Phạm vi:** `/home/hiubao/Projects/03_Work/Company/autoresearching`
> **Trạng thái:** KẾ HOẠCH THỰC THI — chờ approve để chạy trong Code mode.
> **Ghi chú quan trọng:** Human đã chấp nhận **có thể mất dữ liệu phát sinh** khi reset; mọi thao tác vẫn phải snapshot + giữ nhánh backup trước khi reset.

---

## 1. Mục tiêu

1. Xác định **Docker gốc/chuẩn** đang làm nền (bản của sếp): 1 container gánh 4 listener.
2. Reset cấu hình Docker về đúng chuẩn gốc, loại bỏ (archive) cấu hình bản mới gây nhiễu.
3. So sánh **bản mới/nhánh mới** với **bản gốc**, chọn đúng phần **thực sự cần thiết** để hệ thống chạy bình thường, chuyển các phần đó về bản gốc thay vì giữ nguyên bản mới.
4. Bỏ các sửa đổi thử nghiệm không cần thiết nhưng **không làm hỏng luồng chạy**.
5. Sau khi xong: build → up → health check → liệt kê file đã đổi, phần đã chuyển, dữ liệu đã reset, kết quả xác minh.

---

## 2. Phạm vi

### 2.1. Trong phạm vi

| Nhóm | File |
|---|---|
| Compose | `docker-compose*.yaml` (7 file), `Dockerfile`, `.dockerignore`, `.env.example` |
| Shell | `deploy.sh`, `scripts/deploy/*.sh`, `scripts/setup_and_run.sh`, `scripts/start_litellm.sh`, `scripts/seed_infra.py` |
| Jenkins | `Jenkinsfile`, `Jenkinsfile-{doc,write,chat,enhance}` |
| Code cần đối chiếu giữa bản mới/gốc | `listener.py`, `*_listener.py`, `main*.py`, `utils.py`, `get_llm_response.py`, `topics.py`, `requirements.txt`, `document_setup/src/schemas/*.py`, `data_analysis/src/modules/analyzer.py`, `document_setup/src/modules/ingest_docs.py`, `test/` |
| Tài liệu | `README.md`, `DOCS_INDEX.md`, `PROJECT_OVERVIEW.md`, `AUTORESEARCHING.md`, `DEPLOY.md`, `DEPLOY_AND_REPORT_RUNBOOK.md`, 20 file `.md` báo cáo cũ |

### 2.2. Dữ liệu runtime được phép reset (Human đã chấp nhận)

| Đối tượng | Cho phép | Giới hạn |
|---|---|---|
| Container dự án (`autoresearching-*`) | `down` / `rm` | Chỉ container, không ảnh hưởng stack khác |
| Volume local của dự án (`autoresearching_*`, `kafka_data`, `mongo_data`, `qdvrant_data`...) | `down -v` | Chỉ sau khi snapshot + ghi danh sách volume vào MANIFEST |
| Dữ liệu phát sinh khi test | được mất | Chấp nhận |
| `.env`, `model_manager/`, code Python có logic nghiệp vụ | **KHÔNG reset** | Chỉ sửa nếu là phần cần chuyển |

---

## 3. Đối tượng ảnh hưởng

| Bên | Ảnh hưởng |
|---|---|
| DevOps/VPS | Lệnh deploy quay về `docker compose -f docker-compose.yaml up -d` |
| BE Gateway | **Topic quyết định**: bản gốc nghe topic production, bản mới nghe `_local` — phải xác nhận (Gate G0) |
| Jenkins | Không sửa Jenkinsfile; xác nhận file compose gốc khớp pipeline |
| AI Dev local | Mất `docker-compose-local.yaml`/`deploy.sh --infra-only`; có lệnh thay thế |
| FE/Admin | Mất Model Manager 8088 nếu giữ nguyên bản gốc (Gate G3) |

---

## 4. Nguyên tắc an toàn

1. **Luôn có nhánh backup trước khi reset:** `git branch backup/pre-cleanup-<date>`.
2. **Snapshot runtime trước:** container/image/volume/ports.
3. **Chỉ reset thứ đã ghi trong MANIFEST** — không `rm` ngoài danh sách.
4. **Chuyển code từ bản mới bằng `git checkout <new> -- <file>`** (giữ history) thay vì copy tay.
5. **1 commit cuối cùng** sau khi mọi thứ pass health check.

---

## 5. Xác định Docker gốc (kết quả rà soát)

| File | Vai trò | Quyết định |
|---|---|---|
| [`docker-compose.yaml`](../docker-compose.yaml:1) | All-in-one: 1 service `autoresearching-ai-dev`, `python main.py` (4 listener), topic production | **GỐC — giữ** |
| [`docker-compose-doc.yaml`](../docker-compose-doc.yaml:1) + `write/chat/enhance` | 1 service/container, `main_*.py` | **GỐC — giữ** (Jenkinsfile tách service) |
| [`Dockerfile`](../Dockerfile:1), [`.dockerignore`](../.dockerignore:1), [`.env.example`](../.env.example:1) | Build chung | **GỐC — giữ (sau khi đối chiếu nội dung bản mới/gốc)** |
| [`Jenkinsfile`](../Jenkinsfile:37) | Build tag `autoresearching-ai-dev` → deploy `docker-compose.yaml` | **GỐC** |
| [`docker-compose-app.yaml`](../docker-compose-app.yaml:1) | 5 service, `main_*_local.py`, topic `_local` | **BẢN MỚI — archive** |
| [`docker-compose-local.yaml`](../docker-compose-local.yaml:1) | 4 service dev `_local` | **BẢN MỚI — archive** |
| [`deploy.sh`](../deploy.sh:1) + `scripts/deploy/*` | Lớp tool mới | **BẢN MỚI — archive** |
| [`docker-compose-infra.yaml`](../docker-compose-infra.yaml:1) | Bootstrap 7 service hạ tầng | **GIỮ TẠI CHỖ** (không phải prod; Gate G1) |

---

## 6. Xác định bản mới/nhánh mới & các thay đổi cần đối chiếu

### 6.1. Quy trình khám phá (chạy trong Code mode)

```bash
git branch -a
git log --oneline -20
git log --oneline -20 --all

# Tìm commit giới thiệu bản mới (docker-compose-app.yaml / deploy.sh)
git log --oneline -- docker-compose-app.yaml deploy.sh scripts/deploy

# BASE = commit ngay trước commit giới thiệu bản mới
# NEW  = HEAD hiện tại (hoặc nhánh mới nếu có)
git diff --stat BASE NEW
git diff BASE NEW -- docker-compose*.yaml deploy.sh scripts/ Dockerfile requirements.txt
git diff BASE NEW -- '*.py'
```

### 6.2. Danh sách thay đổi đã biết (từ tài liệu, chờ `git diff` xác nhận)

| File | Thay đổi đã biết | Nguồn |
|---|---|---|
| [`document_setup/src/schemas/search_papers.py`](../document_setup/src/schemas/search_papers.py:5) | `user_query: str | list[str]` → `str` (fix Gemini `anyOf`) | [`BAO_CAO_TRIEN_KHAI_HA_TANG.md`](../BAO_CAO_TRIEN_KHAI_HA_TANG.md:109) |
| [`requirements.txt`](../requirements.txt:1) | thêm `tavily-python` (thiếu), `pytest` | runbook + BAO_CAO |
| [`Dockerfile`](../Dockerfile:1) | base 3.12 + `r-base` + `gfortran` + BLAS/LAPACK | runbook |
| `write_listener.py`, `utils.py`, `chatbot_listener.py`, `document_setup_listener.py`, `data_analysis/src/modules/analyzer.py`, `document_setup/src/modules/ingest_docs.py` | 8 fix + regression (response luôn publish, echo identifier, delete_report, health-check) | [`DEPLOY_AND_REPORT_RUNBOOK.md`](../DEPLOY_AND_REPORT_RUNBOOK.md:12) |
| `test/` 4 file mới | `test_ai_fixes*.py`, `test_minio_path.py`, `test_structured_output_fallback.py` | runbook |

### 6.3. Tiêu chí "cần thiết" vs "không cần thiết"

| Tiêu chí cần thiết (chuyển về gốc) | Tiêu chí không cần thiết (bỏ) |
|---|---|
| Fix lỗi runtime/lỗi parse schema/contract | File `.md` báo cáo AI |
| Thiếu dependency làm build/test fail | `deploy.sh` + `scripts/deploy/*` |
| Fix làm BE nhận response thay vì im lặng | `docker-compose-app/local.yaml` (bản mới) |
| Fix tương thích Python 3.12 (PEP 701) | `BAO_CAO_*`, `HANDOFF_*`, `AI_TICKET_*` |
| Test regression bảo vệ fix | Tính năng thử nghiệm chưa dùng (`model_manager` nếu Gate G3 = không dùng) |

---

## 7. Cổng quyết định (chốt trước khi chạy)

| Gate | Câu hỏi | Mặc định an toàn |
|---|---|---|
| **G0** | BE gửi topic production hay `_local`? | Nếu `_local`: **KHÔNG bỏ `app.yaml`** tới khi BE xác nhận; nếu production: bỏ được |
| **G1** | Hạ tầng dự án tự chạy hay công ty cấp? | Giữ `infra.yaml` tại chỗ, thêm banner `LOCAL ONLY` |
| **G2** | Có dùng LiteLLM/model `localhost`/`groq`? | Test scripts dùng `localhost` ⇒ mặc định **giữ** `litellm` |
| **G3** | FE/Admin có dùng Model Manager 8088? | Mặc định **giữ** `model_manager/` (chỉ là code + 1 container tùy chọn) |

---

## 8. Kế hoạch thực thi (S0–S14)

### S0 — Snapshot + nhánh backup

```bash
cd /home/hiubao/Projects/03_Work/Company/autoresearching
mkdir -p .cleanup_snapshot && date -Is > .cleanup_snapshot/timestamp.txt
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}" > .cleanup_snapshot/docker_ps_before.txt
docker compose ls -a > .cleanup_snapshot/compose_ls_before.txt
docker volume ls > .cleanup_snapshot/volumes_before.txt
git status --porcelain > .cleanup_snapshot/git_status_before.txt
git branch -a > .cleanup_snapshot/branches_before.txt
git log --oneline -5 > .cleanup_snapshot/git_log_before.txt
sha256sum docker-compose*.yaml Dockerfile deploy.sh Jenkinsfile* > .cleanup_snapshot/sha256_before.txt
git branch backup/pre-cleanup-$(date +%Y%m%d-%H%M%S)
```

### S1 — Xác định BASE/NEW (theo 6.1)

Ghi kết quả `BASE`, `NEW` vào `MANIFEST`. Nếu chỉ có 1 nhánh: `NEW=HEAD`, `BASE=git log -- docker-compose.yaml | parent`.

### S2 — Tạo backup thư mục + MANIFEST

```bash
mkdir -p backup/docker_obsolete backup/docs_obsolete
cat > backup/MANIFEST_$(date +%Y%m%d).md <<'EOF'
# Manifest
| File nguồn | File đích | sha256 | Lý do | Rollback |
|---|---|---|---|---|
EOF
```

### S3 — Archive cấu hình Docker bản mới

```bash
# tracked → giữ history
git mv docker-compose-app.yaml backup/docker_obsolete/
git mv docker-compose-local.yaml backup/docker_obsolete/
git mv deploy.sh backup/docker_obsolete/
git mv scripts/deploy backup/docker_obsolete/scripts-deploy
# nếu G1 = "cấp sẵn" → git mv docker-compose-infra.yaml backup/docker_obsolete/
```

### S4 — Chuyển các phần CẦN THIẾT từ bản mới về bản gốc

```bash
# Áp dụng từng file theo bảng 6.2, chỉ sau khi git diff xác nhận nội dung:
git checkout NEW -- document_setup/src/schemas/search_papers.py
git checkout NEW -- requirements.txt
git checkout NEW -- Dockerfile
git checkout NEW -- write_listener.py utils.py chatbot_listener.py document_setup_listener.py
git checkout NEW -- data_analysis/src/modules/analyzer.py document_setup/src/modules/ingest_docs.py
git checkout NEW -- test/
```

**Lưu ý bắt buộc:** KHÔNG `git checkout NEW -- docker-compose-app.yaml deploy.sh scripts/deploy` — đây là phần bỏ đi.

### S5 — Kiểm tra diff sau khi chuyển

```bash
git diff BASE --stat
git diff BASE -- '*.py' | head -200
```

### S6 — Sửa `.sh` theo luồng gốc

- `deploy.sh` mới (≤80 dòng): subcommand `up|down|restart|build|logs|status`, chỉ dùng `docker-compose.yaml`, image `autoresearching-ai-dev:latest`.
- `scripts/setup_and_run.sh`: gọi `./deploy.sh up`.
- `scripts/start_litellm.sh`: giữ nếu G2 = giữ; nếu không → archive.

### S7 — Move 20 file `.md` báo cáo cũ vào `backup/docs_obsolete/`

Dùng `mv` (đa số bị `.gitignore`, không tracked). Cập nhật MANIFEST.

### S8 — Cập nhật tài liệu

| File | Việc |
|---|---|
| [`DEPLOY.md`](../DEPLOY.md:1) | Viết lại: build tag, `docker compose -f docker-compose.yaml up -d`, topic production, bảng cổng |
| [`README.md`](../README.md:37) | Xóa `--skip-build`; image tag đúng; lệnh vận hành đúng container |
| [`DOCS_INDEX.md`](../DOCS_INDEX.md:1) | Xóa link báo cáo cũ; xử lý link `docs/*` chết |

### S9 — Reset dữ liệu phát sinh (Human đã chấp nhận)

```bash
# Dừng stack cũ (nếu còn chạy) — KHÔNG dừng stack khác ngoài autoresearching
docker compose -f backup/docker_obsolete/docker-compose-app.yaml down 2>/dev/null || true
docker compose -f docker-compose.yaml down 2>/dev/null || true

# Reset volume local của dự án — CHỈ chạy khi đã có snapshot + MANIFEST
# (liệt kê trước bằng: docker volume ls | grep autoresearching)
# docker volume rm autoresearching_kafka_data autoresearching_redis_data ...  # chọn lọc
```

### S10 — Build image

```bash
docker build -t autoresearching-ai-dev:latest -f Dockerfile .
```

### S11 — Up hạ tầng (nếu G1 = tự chạy) + app

```bash
# G1 = tự chạy:
docker compose -f docker-compose-infra.yaml up -d
# app gốc:
docker compose -f docker-compose.yaml up -d
```

### S12 — Health check

```bash
docker ps --filter "name=autoresearching" --format "table {{.Names}}\t{{.Status}}"
docker logs --tail 100 autoresearching-ai-dev | grep -E "Ingestor loop started" | wc -l   # kỳ vọng 4
# Kiểm tra port hạ tầng nếu có: 9092 6379 27017 6333 9000
```

### S13 — Chạy test

```bash
docker exec autoresearching-ai-dev python -m pytest -q test/   # kỳ vọng pass, không fail
# Smoke chỉ khi G0 = production và đã cấu hình model/key
```

### S14 — Commit + báo cáo

```bash
git add -A
git status --porcelain
git commit -m "chore(deploy): reset docker to canonical, port necessary fixes from new branch"
```

---

## 9. Rủi ro, tác động, rollback

| Bước | Rủi ro | Rollback |
|---|---|---|
| S3 archive compose | Nhầm file gốc | `git mv` ngược + `git checkout BASE -- docker-compose.yaml` |
| S4 chuyển code | Chuyển nhầm phần thử nghiệm | `git checkout BASE -- <file>` cho file không cần |
| S9 reset volume | Mất dữ liệu thật | Chỉ reset volume đã ghi MANIFEST; nếu nghi ngờ → bỏ qua S9, giữ volume |
| S10 build | Build fail do dependency | Khôi phục `Dockerfile`/`requirements.txt` từ BASE, xem log pip |
| S11 up | Container crash do thiếu hạ tầng/topic | `docker logs` + kiểm tra Gate G0/G1 |
| Chung | Jenkins `prune -a -f` chạy giữa lúc dọn | Tránh khung giờ Jenkins |

---

## 10. Acceptance criteria

| # | Tiêu chí | Lệnh kiểm tra | Kết quả mong đợi |
|---|---|---|---|
| A1 | Chỉ còn compose gốc | `ls docker-compose*.yaml` | `docker-compose.yaml` + 4 file per-service |
| A2 | `deploy.sh` hợp lệ, không lệnh cấm | `bash -n deploy.sh && shellcheck deploy.sh` | exit 0 |
| A3 | Image build thành công | `docker images | grep autoresearching-ai-dev` | tag `latest` tồn tại |
| A4 | Container gốc Up | `docker ps | grep autoresearching-ai-dev` | status Up |
| A5 | 4 listener khởi động | `docker logs autoresearching-ai-dev \| grep "Ingestor loop started" \| wc -l` | 4 |
| A6 | Test pass | `docker exec autoresearching-ai-dev python -m pytest -q test/` | không fail |
| A7 | Không còn reference bản mới | `grep -rn "docker-compose-app.yaml\|docker-compose-local.yaml\|scripts/deploy/" --include="*.sh" --include="*.md" .` | rỗng |
| A8 | MANIFEST đầy đủ | `cat backup/MANIFEST_*.md` | có sha256 + rollback |
| A9 | 20 `.md` cũ đã archive | `ls backup/docs_obsolete \| wc -l` | ≥ 20 |
| A10 | 1 commit | `git log --oneline -1` | `chore(deploy): ...` |

---

## 11. Báo cáo cuối (đầu ra bắt buộc sau khi chạy)

1. Danh sách file đã thay đổi (`git diff BASE --stat`).
2. Danh sách phần đã chuyển từ bản mới → bản gốc (mục 6.2, kèm sha).
3. Danh sách dữ liệu/cấu hình bị reset (volume/container đã xóa).
4. Kết quả build/up/healthcheck/test (A3–A6).
5. File nào thuộc diện "thử nghiệm" đã loại bỏ.

---

## 12. Lệnh cấm (vẫn giữ, trừ S9 đã được phép)

```bash
rm -rf <bất kỳ>          # chỉ mv/git mv
git clean -fd            # không dọn untracked
docker system prune -a   # không chạy tay (Jenkins đang có lệnh này)
# down -v / volume rm CHỈ trong S9, với volume đã liệt kê trong MANIFEST
```
