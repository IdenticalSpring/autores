# 🚀 DEPLOY — AUTORESEARCHING (Docker chuẩn)

> Kiến trúc chuẩn: **1 container `autoresearching-ai-dev`** chạy [`main.py`](main.py) với 4 listener
> (Document Setup, Write, Chatbot, Enhancement) trong **một process** (`asyncio.gather`).
> Topic sử dụng là **topic production** (`*_request` / `*_response`).

---

## 1. File Docker chính thức

| File | Vai trò |
|---|---|
| [`Dockerfile`](Dockerfile) | Build image `autoresearching-ai-dev:latest` (Python 3.12 + r-base + BLAS/LAPACK) |
| [`docker-compose.yaml`](docker-compose.yaml) | **Chạy chuẩn**: 1 service `autoresearching-ai-dev`, `python main.py` |
| `docker-compose-doc.yaml`, `-write.yaml`, `-chat.yaml`, `-enhance.yaml` | Deploy tách riêng từng service (dùng bởi `Jenkinsfile-*`) |
| [`docker-compose-infra.yaml`](docker-compose-infra.yaml) | **Chỉ dùng cho local** — Kafka, Redis Stack, MongoDB, Qdrant, MinIO, Kafka UI, LiteLLM |
| [`Jenkinsfile`](Jenkinsfile) | CI/CD: build tag `autoresearching-ai-dev` → `docker-compose up -d` |

Cấu hình cũ (`docker-compose-app.yaml`, `docker-compose-local.yaml`, `deploy.sh` bản cũ, `scripts/deploy/*`)
được lưu trữ tại `backup/docker_obsolete/` — **không dùng để deploy**.

---

## 2. Biến môi trường

```bash
cp .env.example .env
nano .env     # GEMINI_KEY / OPEN_AI_KEY / GROQ_API_KEY / SERP_KEY / REDIS_PWD / ...
```

Bắt buộc tối thiểu: `KAFKA`, `REDIS_HOST/REDIS_PORT/REDIS_PWD`, `MONGO_*`, `MINIO_*`, `QDRANT_*`.

---

## 3. Triển khai

### 3.1. Hạ tầng đã có sẵn (VPS công ty cấp)

```bash
./deploy.sh build      # build image autoresearching-ai-dev:latest
./deploy.sh up         # docker compose -f docker-compose.yaml up -d
./deploy.sh health
```

### 3.2. Hạ tầng chạy local bằng Docker

```bash
./deploy.sh infra-up   # Kafka + Redis + Mongo + Qdrant + MinIO + Kafka UI + LiteLLM
./deploy.sh all        # build + up app + health
```

### 3.3. Chạy trực tiếp không qua script

```bash
docker build -t autoresearching-ai-dev:latest -f Dockerfile .
docker compose -f docker-compose.yaml up -d
docker logs -f autoresearching-ai-dev
```

---

## 4. Kiểm tra sau deploy

```bash
docker ps --filter name=autoresearching

# Phải thấy đúng 4 dòng "Ingestor loop started." (1 dòng cho mỗi listener)
docker logs autoresearching-ai-dev 2>&1 | grep -c "Ingestor loop started"

# Cổng hạ tầng (nếu chạy local)
for p in 9092 6379 27017 6333 9000; do
  timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$p" && echo "port $p OK" || echo "port $p FAIL";
done
```

Test tích hợp Kafka (mặc định dùng **topic production**; thêm `--local` nếu chạy `main_*_local.py`):

```bash
docker exec autoresearching-ai-dev python scripts/test_kafka_search_papers.py
docker exec autoresearching-ai-dev python scripts/smoke_admin_documents.py --timeout 300
```

---

## 5. Bảng cổng & giao diện

| Dịch vụ | Port | Ghi chú |
|---|---|---|
| Kafka broker | 9092 | Event bus |
| Kafka UI | 8080 | Xem topic/message (local) |
| Redis Stack | 6379 | Cache + LangGraph checkpoint |
| MongoDB | 27017 | Document/chat history |
| Qdrant | 6333 / 6334 | Vector DB (RAG) |
| MinIO API / Console | 9000 / 9001 | Object storage |
| LiteLLM Proxy | 8000 | Chỉ cần khi dùng model `groq`/`oss`/`local` |
| Model Manager (tùy chọn) | 8088 | Web quản lý model/API key — không thuộc compose chuẩn |

Chạy Model Manager khi cần (dùng chính image chuẩn):

```bash
docker run -d --name autoresearching-model-manager --network host \
  --env-file .env -v "$PWD/.env:/app/.env:ro" \
  autoresearching-ai-dev:latest \
  uvicorn model_manager.main:app --host 0.0.0.0 --port 8088
```

---

## 6. Vận hành

```bash
./deploy.sh status      # container + port
./deploy.sh logs doc    # log realtime
./deploy.sh restart     # restart container app
./deploy.sh stop        # dừng (giữ volume)
./deploy.sh infra-down  # dừng hạ tầng local (giữ volume)
```

> ⚠️ Không dùng `docker compose down -v` trên môi trường có dữ liệu thật — lệnh này xóa volume.
