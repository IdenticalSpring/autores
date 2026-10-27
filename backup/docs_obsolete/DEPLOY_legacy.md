# 🚀 HƯỚNG DẪN TRIỂN KHAI HỆ THỐNG AUTORESEARCHING TRÊN VPS

> Tài liệu này dành cho Quản trị viên hệ thống (DevOps / Tech Lead / Sếp) để triển khai toàn bộ hệ thống AI Backend **autoresearching** trên một máy chủ VPS mới từ đầu một cách tự động, chuẩn hóa và độc lập (không phụ thuộc vào Harbor registry cũ).

---

## 📋 1. TỔNG QUAN KIẾN TRÚC & DỊCH VỤ

Hệ thống được thiết kế theo kiến trúc **Event-Driven Microservices** giao tiếp qua **Apache Kafka**.

```
                   ┌─────────────────────────────────────────┐
                   │          Apache Kafka (Broker)          │
                   │         Topic Request & Response        │
                   └─────┬─────────────────────────────▲─────┘
                         │                             │
    ┌────────────────────┼─────────────────────────────┼────────────────────┐
    │                    │                             │                    │
    ▼                    ▼                             ▼                    ▼
┌──────────────┐  ┌──────────────┐              ┌──────────────┐  ┌────────────────┐
│ Service DOC  │  │ Service WRITE│              │ Service CHAT │  │Service ENHANCE │
│ (Setup paper,│  │(Viết bài báo,│              │(Inner/Outer/ │  │(Nâng cao văn   │
│ outline,...) │  │ slide,...)   │              │ Writer bot)  │  │ bản, gợi ý)    │
└──────┬───────┘  └──────┬───────┘              └──────┬───────┘  └────────┬───────┘
       │                 │                             │                   │
       └─────────────────┴──────────────┬──────────────┴───────────────────┘
                                        ▼
    ┌───────────────────────────────────────────────────────────────────────┐
    │                         HẠ TẦNG DỮ LIỆU & CACHE                       │
    │  - Redis Stack: Lưu cache, AI_KEY, session state, LangGraph checkpoint│
    │  - MongoDB: Lưu trữ metadata, lịch sử chat, user research docs        │
    │  - Qdrant Vector DB: Lưu trữ embedding vector tìm kiếm tài liệu       │
    │  - MinIO (S3): Lưu file đính kèm, pdf, tài liệu người dùng            │
    └───────────────────────────────────────────────────────────────────────┘
```

---

## 💻 2. YÊU CẦU PHẦN CỨNG VPS TỐI THIỂU

| Thành phần | Cấu hình khuyến nghị | Tối thiểu |
|---|---|---|
| **CPU** | 4 Cores (hoặc 8 vCPU) | 2 Cores |
| **RAM** | 16 GB | 8 GB |
| **Ổ cứng** | 100 GB SSD NVMe | 50 GB SSD |
| **Hệ điều hành** | Ubuntu 22.04 LTS hoặc 24.04 LTS | Ubuntu 20.04+ |
| **Docker** | Docker Engine 24.0+ & Docker Compose v2.20+ | Docker v20+ |

---

## 🛠️ 3. CÀI ĐẶT MÔI TRƯỜNG TRÊN VPS TRẮNG

Chạy các lệnh sau trên terminal của VPS (với quyền `root` hoặc `sudo`):

```bash
# 1. Cập nhật hệ thống
sudo apt update && sudo apt upgrade -y

# 2. Cài đặt các công cụ cơ bản (KHÔNG cần cài Python hay pip/venv trên host)
sudo apt install -y curl wget git jq net-tools

# 3. Cài đặt Docker Engine & Docker Compose Plugin chuẩn từ Docker official
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# 4. Thêm user hiện tại vào group docker (để chạy docker không cần sudo)
sudo usermod -aG docker $USER
newgrp docker

# 5. Kiểm tra phiên bản Docker & Docker Compose
docker --version
docker compose version
```

---

## ⚡ 4. TRIỂN KHAI TỰ ĐỘNG VỚI DEPLOY.SH (INTERACTIVE MENU & DOCKER-FIRST)

Hệ thống cung cấp bộ công cụ [`deploy.sh`](file:///home/hiubao/Projects/03_Work/Company/autoresearching/deploy.sh) được chuẩn hóa **100% Docker-first**. Toàn bộ tác vụ từ khởi động hạ tầng, build image, nạp dữ liệu khởi tạo (seed Redis, MinIO, Kafka) đến chạy kiểm thử đều chạy bên trong Docker container mà **không cần bất kỳ môi trường ảo Python (venv) nào trên máy chủ VPS**.

### 🌟 4.1. Giao diện Menu Tương tác (Interactive CLI Menu)

Chỉ cần gõ lệnh không kèm tham số:

```bash
chmod +x deploy.sh
./deploy.sh
```

Menu điều khiển trực quan sẽ hiển thị trên terminal:

```text
╔════════════════════════════════════════════════════════════╗
║    AUTORERESEARCHING — QUẢN TRỊ & TRIỂN KHAI DOCKER-FIRST    ║
╚════════════════════════════════════════════════════════════╝
  [1]  🚀 Full Deploy (Infra + Build Image + Seed + 4 AI Services)
  [2]  🏗️  Khởi động Hạ tầng (Kafka, Redis, Mongo, Qdrant, MinIO, LiteLLM)
  [3]  🐳 Build Docker Image (autoresearching-ai:latest)
  [4]  🌱 Seed Dữ liệu Hạ tầng qua Docker (AI_KEY, MinIO, Topics)
  [5]  🤖 Khởi động 4 AI Services (Doc, Write, Chat, Enhance)
  [6]  📊 Kiểm tra Trạng thái & Cổng dịch vụ (Health Check)
  [7]  📜 Xem Logs thời gian thực (Interactive Log Viewer)
  [8]  🔄 Khởi động lại Dịch vụ (Restart Menu)
  [9]  🧪 Chạy Test tích hợp Kafka qua Docker (Không cần venv)
  [10] 🛑 Dừng Hệ thống (Stop / Down)
  [0]  🚪 Thoát
────────────────────────────────────────────────────────────
👉 Chọn tác vụ [0-10]:
```

### ⚡ 4.2. Chế độ Chạy Tự động Bằng CLI Flags (Dành cho CI/CD hoặc Headless VPS)

| Cờ lệnh (Flag) | Mục đích |
|---|---|
| `./deploy.sh --all` | Chạy toàn bộ quy trình: Kiểm tra + Hạ tầng + Build + Seed qua Docker + 4 AI Services + Health check |
| `./deploy.sh --infra-only` | Chỉ khởi động cụm hạ tầng (Kafka, Redis, Mongo, Qdrant, MinIO, LiteLLM) |
| `./deploy.sh --app-only` | Chỉ khởi động 4 AI Service (Doc, Write, Chat, Enhance) |
| `./deploy.sh --build` | Rebuild image `autoresearching-ai:latest` từ Dockerfile |
| `./deploy.sh --seed` | Chạy `seed_infra.py` bên trong Docker container (nạp Redis AI_KEY, S3 buckets, Kafka topics) |
| `./deploy.sh --status` | Quét trạng thái containers và kiểm tra kết nối các cổng mạng |
| `./deploy.sh --logs [service]` | Xem log realtime (vd: `./deploy.sh --logs doc` hoặc `./deploy.sh --logs`) |
| `./deploy.sh --restart [target]` | Restart dịch vụ (vd: `./deploy.sh --restart app` hoặc `./deploy.sh --restart doc`) |
| `./deploy.sh --test` | Mở menu chạy các kịch bản kiểm thử Kafka qua Docker |
| `./deploy.sh --down` | Dừng an toàn toàn bộ hệ thống (App + Hạ tầng) |

---

## 🛠️ 4. QUY TRÌNH TRIỂN KHAI THỦ CÔNG (TÙY CHỌN NẾU KHÔNG DÙNG SCRIPT)
cd /opt
# hoặc git clone git@github.com:your-company/autoresearching.git
cd autoresearching

# 2. Tạo file .env từ file mẫu
cp .env.example .env

# 3. Mở file .env để điền API key thật
nano .env
```

> ⚠️ **CÁC BIẾN QUAN TRỌNG CẦN ĐIỀN TRONG `.env`:**
> - `GEMINI_KEY`: API key Google Gemini (lấy tại Google AI Studio: https://aistudio.google.com/app/apikey).
> - `OPEN_AI_KEY`: API key OpenAI (nếu dùng GPT-4o).
> - `SERP_KEY`: API key SerpAPI (dùng cho tìm kiếm web).
> - Nếu chạy production: Đổi `REDIS_PWD`, `MONGO_INITDB_ROOT_PASSWORD`, `MINIO_SECRET_ACCESS_KEY` thành mật khẩu an toàn.

---

### Bước 2: Khởi động Hạ tầng Dữ liệu (Infrastructure)

Chạy cụm Docker hạ tầng gồm Kafka, Redis Stack, MongoDB, Qdrant, MinIO và Kafka UI:

```bash
# Chạy toàn bộ hạ tầng ngầm (-d)
docker compose -f docker-compose-infra.yaml up -d

# Kiểm tra trạng thái các container
docker compose -f docker-compose-infra.yaml ps
```

*Đợi khoảng 15-30 giây để tất cả 6 container khởi động và đạt trạng thái `healthy`.*

---

### Bước 3: Nạp dữ liệu khởi tạo (Seed Data vào Redis & MinIO qua Docker)

Hệ thống cần các API key trong Redis (`AI_KEY:*`) và các S3 buckets sẵn có trong MinIO. Bạn nạp dữ liệu hoàn toàn bằng Docker mà không cần cài Python/venv:

```bash
# Nạp dữ liệu tự động qua Docker container:
./deploy.sh --seed

# Hoặc nếu chạy thủ công qua lệnh docker run:
docker run --rm --network host --env-file .env -v "$(pwd)":/app autoresearching-ai:latest python scripts/seed_infra.py
```

*Kết quả mong đợi:*
- `✓ Đã nạp Redis key: AI_KEY:* vào Redis`
- `✓ Đã tạo/kiểm tra 4 MinIO buckets: autoresearching, analysis, documents, users`
- `✓ MongoDB & Qdrant kết nối thành công!`

---

### Bước 4: Build Image & Khởi chạy 4 AI Services

Hệ thống tự build base image từ `Dockerfile` chuẩn (`python:3.12-slim` tích hợp R và Fortran để phân tích thống kê) mà không cần Harbor registry.

```bash
# Build và chạy 4 services: doc, write, chat, enhance
docker compose -f docker-compose-app.yaml up -d --build

# Kiểm tra trạng thái 4 services
docker compose -f docker-compose-app.yaml ps
```

*Kiểm tra logs xem các service đã kết nối Kafka thành công chưa:*
```bash
docker logs --tail 30 autoresearching-doc
docker logs --tail 30 autoresearching-write
docker logs --tail 30 autoresearching-chat
docker logs --tail 30 autoresearching-enhance
```

---

### Bước 5: Chạy Smoke Test kiểm tra End-to-End (Qua Docker)

Chạy script giả lập gửi request tìm kiếm bài báo khoa học qua Kafka bằng Docker:

```bash
# Chạy smoke test qua menu test của deploy.sh:
./deploy.sh --test

# Hoặc chạy trực tiếp script test mong muốn qua Docker container:
docker run --rm -it --network host --env-file .env -v "$(pwd)":/app autoresearching-ai:latest python scripts/test_kafka_search_papers.py
```

*Nếu điền đúng API key thật vào `.env`, script sẽ trả về danh sách bài báo khoa học từ OpenAlex thành công!*

---

## 🌐 5. DANH SÁCH CỔNG DỊCH VỤ & GIAO DIỆN QUẢN TRỊ

| Dịch vụ | Cổng (Port) | Tài khoản / Mật khẩu mặc định | Mục đích |
|---|---|---|---|
| **Kafka UI** | `http://<IP_VPS>:8080` | Không cần login | Giao diện web quản lý topic, partition, message Kafka |
| **MinIO Console** | `http://<IP_VPS>:9001` | `minioadmin` / `minioadmin_local` | Quản lý file, bucket S3 lưu trữ |
| **MinIO S3 API** | `http://<IP_VPS>:9000` | S3 API Endpoint | App upload/download file |
| **Qdrant Dashboard** | `http://<IP_VPS>:6333/dashboard` | Không cần login | Giao diện vector database Qdrant |
| **Redis Stack** | `6379` | pass: `redis_password_local` | Cache & LangGraph Checkpointer |
| **MongoDB** | `27017` | `root` / `mongo_password_local` | Document database |
| **Kafka Broker** | `9092` | PLAINTEXT | Message broker |

> 🔒 **Lưu ý bảo mật tường lửa (UFW):**
> Chỉ nên mở port `8080` (Kafka UI) hoặc `9001` (MinIO) cho IP của nội bộ công ty. Các port cơ sở dữ liệu `9092, 6379, 27017, 6333, 9000` chỉ nên để lắng nghe nội bộ trên VPS (`localhost` hoặc dải mạng riêng).

---

## 🔧 6. VẬN HÀNH & BẢO TRÌ

### Xem logs thời gian thực
```bash
# Xem log toàn bộ app
docker compose -f docker-compose-app.yaml logs -f

# Xem log riêng một service (ví dụ service Viết tài liệu)
docker logs -f autoresearching-write
```

### Khởi động lại / Dừng hệ thống
```bash
# Khởi động lại app
docker compose -f docker-compose-app.yaml restart

# Dừng app
docker compose -f docker-compose-app.yaml down

# Dừng toàn bộ hạ tầng
docker compose -f docker-compose-infra.yaml down
```

### Sao lưu dữ liệu (Backup Volumes)
Dữ liệu của tất cả database được lưu an toàn trong Docker Named Volumes:
- `data-kafka`: Dữ liệu message queue
- `data-redis`: Dữ liệu Redis keys & graph checkpoint
- `data-mongo`: Toàn bộ database MongoDB
- `data-qdrant`: Toàn bộ vector collection của Qdrant
- `data-minio`: Toàn bộ file đính kèm S3

Đường dẫn lưu trữ trên Ubuntu thường ở: `/var/lib/docker/volumes/autoresearching_data-*`. Khi cần backup, chỉ cần dùng lệnh `tar` nén các thư mục này hoặc dùng công cụ snapshot VPS.

---

## 📌 7. LƯU Ý KHI KẾT NỐI VỚI BACKEND / FRONTEND (CRM)

1. **Kafka Request / Response Topics**:
   - Backend CRM giao tiếp với AI bằng cách push JSON vào các topic request:
     - `document_setup_request` (hoặc `document_setup_request_local` khi test)
     - `write_section_request`
     - `chatbot_request`
     - `enhancement_request`
   - AI sau khi xử lý xong sẽ đẩy kết quả ngược lại topic response tương ứng (`*_response`).
2. **Health Check Callback**:
   - Trong code có tích hợp callback gửi trạng thái tới Backend CRM (`http://<BACKEND_HOST>:4002/api/health-check`). Khi chưa có Backend CRM, service vẫn chạy bình thường và tự động bỏ qua lỗi kết nối này mà không gây gián đoạn luồng xử lý.
3. **Jenkins CI/CD**:
   - Các file `Jenkinsfile-*` hiện đang trỏ về IP server cũ (`192.168.3.140`). Khi sếp cấu hình xong VPS mới, chỉ cần cập nhật lại địa chỉ IP trong các file Jenkinsfile là có thể triển khai tự động qua CI/CD.

---

## 🤖 8. HƯỚNG DẪN CẤU HÌNH & SỬ DỤNG MODEL TỰ HOST (SELF-HOSTED / LOCAL LLM)

Ngoài các API Cloud thương mại (OpenAI, Google Gemini), hệ thống đã được tích hợp sẵn cơ chế gọi **Model tự host** (Local LLM, vLLM, Ollama hoặc LiteLLM Proxy) theo chuẩn OpenAI API.

### 8.1. Cơ chế hoạt động trong mã nguồn
Trong file `get_llm_response.py` (hàm `get_llm()`):
```python
elif model_id == "localhost":
    llm = ChatOpenAI(
        model="localhost",
        api_key="None",
        base_url=settings.LOCALLLM_BASE_URL,
        max_tokens=max_output_tokens,
        temperature=temperature,
    )
```
- Khi nhận yêu cầu có `"model_id": "localhost"`, hệ thống tự động khởi tạo client OpenAI trỏ thẳng tới địa chỉ cấu hình trong biến môi trường `LOCALLLM_BASE_URL`.

### 8.2. Các bước triển khai & cấu hình Model tự host

#### Bước 1: Khởi chạy Model Server (Chuẩn OpenAI API)
Bạn có thể dùng một trong các giải pháp sau:

* **Lựa chọn A: LiteLLM Proxy (Gom nhiều model nội bộ hoặc Groq/Ollama về 1 cổng)**
  ```bash
  docker run -d --name litellm-proxy \
    -p 8000:8000 \
    -e GROQ_API_KEY="your_groq_api_key" \
    ghcr.io/berriai/litellm:main-latest \
    --model groq/openai/gpt-oss-120b --port 8000
  ```

* **Lựa chọn B: vLLM (Chạy trực tiếp model mã nguồn mở trên GPU riêng)**
  ```bash
  python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-72B-Instruct \
    --port 8000
  ```

* **Lựa chọn C: Ollama**
  ```bash
  ollama run qwen2.5:72b
  # Endpoint API chuẩn: http://localhost:11434/v1
  ```

#### Bước 2: Cấu hình biến môi trường trong `.env`
Mở file `.env` trên máy chủ và cập nhật URL trỏ tới máy chủ model tự host:
```bash
# Trỏ tới LiteLLM / vLLM (port 8000)
LOCALLLM_BASE_URL="http://localhost:8000/v1"

# Hoặc nếu chạy Ollama (port 11434)
# LOCALLLM_BASE_URL="http://localhost:11434/v1"
```

#### Bước 3: Cách gọi từ Backend Gateway (Kafka Request)
Khi Backend Gateway gửi request vào bất kỳ Kafka topic nào (ví dụ `document_setup_request`, `write_section_request`):
- Chỉ cần truyền trường `"model_id": "localhost"` trong payload JSON:
```json
{
  "event_type": "generate_domains",
  "document_id": "doc_test_001",
  "user_id": "user_001",
  "session_id": "sess_001",
  "model_id": "localhost",
  "field": "Khoa học máy tính",
  "domains_num": 5,
  "language": "vi",
  "is_tool": false
}
```
Worker AI sẽ tự động phân giải và gửi câu lệnh sang máy chủ model tự host thay vì gọi OpenAI hay Gemini.

