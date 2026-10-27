# 🧠 AutoResearching — AI Backend Microservices

> Hệ thống AI Backend hỗ trợ nghiên cứu khoa học tự động, tạo đề tài, tìm kiếm tài liệu (RAG), viết báo cáo chuyên đề và phân tích dữ liệu thống kê.  
> Kiến trúc **Event-Driven Microservices** giao tiếp bất đồng bộ qua **Apache Kafka**.

---

## 🏗️ Kiến trúc & 4 Dịch vụ AI chính

```
                       ┌───────────────────────────────┐
                       │     Apache Kafka (Broker)     │
                       │   Request / Response Topics   │
                       └───┬───────────┬───────────┬───┘
                           │           │           │
            ┌──────────────┴──┐ ┌──────┴─────┐ ┌───┴──────────────┐
            ▼                 ▼ ▼            ▼ ▼                  ▼
    ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
    │  DOC SERVICE  │ │ WRITE SERVICE │ │ CHAT SERVICE  │ │ENHANCE SERVICE│
    │  (Đề tài, RAG,│ │ (Viết bài báo,│ │ (Chatbot inner│ │ (Gợi ý văn    │
    │   outline)    │ │  slide, v.v.) │ │   & outer)    │ │  bản AI inDoc)│
    └───────┬───────┘ └───────┬───────┘ └───────┬───────┘ └───────┬───────┘
            └─────────────────┼─────────────────┼─────────────────┘
                              ▼                 ▼
                       ┌───────────────────────────────┐
                       │    CƠ SỞ DỮ LIỆU & HẠ TẦNG    │
                       │ • Redis Stack (Cache/State)   │
                       │ • MongoDB (Documents/History) │
                       │ • Qdrant (Vector DB / RAG)    │
                       │ • MinIO (S3 Object Storage)   │
                       │ • LiteLLM Proxy (Groq/Local)  │
                       └───────────────────────────────┘
```

---

## ⚡ Triển khai nhanh (Docker chuẩn)

Kiến trúc chuẩn: **1 container `autoresearching-ai-dev`** chạy `main.py` với 4 listener
(doc / write / chat / enhance) trong một process, dùng **topic production**.

### 1. Chuẩn bị
```bash
# Clone repository
git clone <repo_url> autoresearching
cd autoresearching

# Tạo file cấu hình từ template
cp .env.example .env
nano .env   # Điền GEMINI_KEY / OPEN_AI_KEY / GROQ_API_KEY / SERP_KEY
```

### 2. Chạy Deploy
```bash
chmod +x deploy.sh
./deploy.sh all     # build image + docker-compose.yaml up -d + health check
```

---

## 🎛️ Các lệnh `deploy.sh`

| Lệnh | Mục đích |
|---|---|
| `./deploy.sh build` | Build image `autoresearching-ai-dev:latest` từ `Dockerfile` |
| `./deploy.sh up` | Khởi động app theo `docker-compose.yaml` (1 container, 4 listener) |
| `./deploy.sh infra-up` | Khởi động hạ tầng local (`docker-compose-infra.yaml`) |
| `./deploy.sh all` | `build` + `up` + `health` |
| `./deploy.sh status` / `health` / `logs [name]` / `restart` / `stop` | Vận hành |
| `./deploy.sh infra-down` | Dừng hạ tầng local (giữ volume) |

---

## 🌐 Danh sách Cổng Dịch vụ & Web UI

| Dịch vụ | Địa chỉ Web / Cổng | Tài khoản / Mật khẩu | Ghi chú |
|---|---|---|---|
| **Kafka UI** | `http://<IP_VPS>:8080` | *(Không cần login)* | Quản lý topic, message, consumer group |
| **MinIO Console** | `http://<IP_VPS>:9001` | `minio_admin` / `minio_password_local` | Quản lý file, bucket S3 lưu trữ |
| **Qdrant Dashboard**| `http://<IP_VPS>:6333/dashboard` | *(Không cần login)* | Quản lý Vector collections & RAG |
| **LiteLLM Proxy** | `http://<IP_VPS>:8000` | *(Tích hợp Groq)* | Endpoint chuẩn OpenAI API (`localhost:8000/v1`) |
| **Redis Stack** | `Port 6379` | pass: `redis_password_local` | Cache & LangGraph Checkpointer |
| **MongoDB** | `Port 27017` | `root` / `mongo_password_local` | Document database |
| **Kafka Broker** | `Port 9092` | PLAINTEXT | Broker chính cho event bus |

---

## 🛠️ Quản trị & Vận hành Thường dùng

```bash
# Xem log toàn bộ 4 listener (chung 1 container)
docker logs -f autoresearching-ai-dev

# Đếm số listener đã khởi động (kỳ vọng = 4)
docker logs autoresearching-ai-dev 2>&1 | grep -c "Ingestor loop started"

# Restart chuẩn
docker compose -f docker-compose.yaml restart

# Dừng app + hạ tầng local (KHÔNG dùng -v để giữ volume)
docker compose -f docker-compose.yaml stop
docker compose -f docker-compose-infra.yaml stop

# Nạp lại dữ liệu khởi tạo (Redis keys, S3 Buckets)
python3 scripts/seed_infra.py
```

> Deploy tách riêng từng service (nếu cần): dùng `docker-compose-doc.yaml`, `-write.yaml`,
> `-chat.yaml`, `-enhance.yaml` tương ứng.

---

## 📖 Tài liệu chi tiết khác

- 📑 [DEPLOY.md](DEPLOY.md): Triển khai, health check, bảng cổng, vận hành.
- 📑 [DOCS_INDEX.md](DOCS_INDEX.md): Danh mục tài liệu kỹ thuật & quy chuẩn kiến trúc của từng module.
- 📑 [AGENTS.md](AGENTS.md): Quy chuẩn clean code và tài liệu hướng dẫn dành cho AI coding assistant.
- 🗄️ `backup/docs_obsolete/`: báo cáo/handoff cũ (archive, chỉ để tra cứu — **không dùng làm tài liệu chuẩn**).
