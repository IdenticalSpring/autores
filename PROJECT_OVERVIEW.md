# 📚 PROJECT OVERVIEW — autoresearching

> **Dành cho**: Developer mới / AI Agent muốn hiểu nhanh và bắt đầu phát triển tiếp.
> **Cập nhật**: 2026-10-02

---

## 1. Tổng quan hệ thống

**autoresearching** là hệ thống AI backend tự động hoá quy trình nghiên cứu khoa học, bao gồm:
- Sinh ý tưởng, outline, tìm paper, tìm journal
- Viết báo cáo, chuyên đề, slide
- Phân tích dữ liệu thống kê
- AI Chatbot hỗ trợ nhà nghiên cứu

**Đặc điểm cốt lõi**: Toàn bộ giao tiếp giữa client và server qua **Apache Kafka** (event-driven). **Không có REST API**.

---

## 2. Kiến trúc tổng quát

```
Client (CRM/Frontend)
        │
        ▼  Kafka request topic
  ┌─────────────────┐
  │   *_listener.py │  ← parse event_type → dispatch
  └────────┬────────┘
           │
   ┌───────┴────────┐
   │  modules/*     │  ← LangGraph workflow / AI logic
   └───────┬────────┘
           │
        ▼  Kafka response topic
Client nhận kết quả (async)
```

**4 Listener chính** (chạy song song bằng `asyncio.gather`):

| Listener file | Kafka topic | Event class | Mô tả |
|---|---|---|---|
| `document_setup_listener.py` | `document_setup_request` | `DocumentSetupEvent` | Setup tài liệu, outline, tìm paper |
| `write_listener.py` | `write_section_request` | `WriteEvent` | Viết báo cáo, slide, phân tích |
| `chatbot_listener.py` | `chatbot_request` | `AIEvent` | Chatbot AI |
| `enhance_listener.py` | `enhancement_request` | `EnhancementEvent` | Nâng cao nội dung |

---

## 3. Tech Stack

| Thành phần | Công nghệ |
|---|---|
| Language | Python 3.x, async/await |
| Message Queue | Apache Kafka (`aiokafka`) |
| LLM | OpenAI (`gpt-*`), Google Gemini |
| LLM Orchestration | LangChain + LangGraph |
| LangGraph Checkpointer | Redis (`AsyncRedisSaver`) + MongoDB (`AsyncMongoDBSaver`) |
| Vector DB | Qdrant (`langchain-qdrant`) — collection `admin_user_guide` |
| Document DB | MongoDB (`pymongo` async) — DB: `BotGraph`, `admin` |
| Cache / State | Redis (2 instance: Redis thường + Redis Stack) |
| File Storage | MinIO (S3-compatible) + boto3 |
| Health Check | HTTP PUT → `http://REDIS_HOST:4002/health-check/{id}` |
| Config | `pydantic_settings`, load từ `/app/.env` |
| Containerization | Docker + Docker Compose |
| CI/CD | Jenkins → SSH → Server `192.168.3.140:/u01/autoresearching-ai-dev` |
| Data Analysis | pandas, numpy, scipy, scikit-learn, statsmodels, semopy, rpy2 |

---

## 4. Cấu trúc thư mục (quan trọng)

```
autoresearching/
│
│── [ROOT CORE FILES]
├── topics.py              # Tất cả Kafka topic name constants
├── event_type.py          # Enum events: DocumentSetupEvent, WriteEvent, AIEvent, EnhancementEvent
├── error_type.py          # Error codes + timeout + max_tokens per event
├── status_type.py         # AIStatus: PENDING(1), PROCESSING(2), FINISHED(3), ERROR(4), SUSPENDED(5)
├── exception_type.py      # Custom exception: AIERROR(status_code, message)
├── error_handle.py        # Centralized error handling
├── producers.py           # Kafka producer helper
├── listener.py            # Base class: BaseAsyncListener (abstract)
├── utils.py               # Singletons: MongoDB, Redis, MinIO, Qdrant, S3
├── get_llm_response.py    # LLM wrapper (OpenAI/Gemini)
├── translate.py           # Dịch thuật
│
│── [LISTENERS]
├── document_setup_listener.py  # ~86KB – dispatch DocumentSetupEvent
├── write_listener.py           # ~98KB – dispatch WriteEvent
├── chatbot_listener.py         # ~43KB – dispatch AIEvent
├── enhance_listener.py         # ~21KB – dispatch EnhancementEvent
│
│── [ENTRY POINTS]
├── main.py               # Prod: chạy 4 listener song song
├── main_chat.py/_local   # Chatbot production/local
├── main_doc.py/_local    # Document Setup production/local
├── main_write.py/_local  # Write production/local
├── main_enhance.py/_local # Enhancement production/local
│
│── [MODULES]
├── document_setup/src/
│   ├── configs/app.py         # AppSettings (DÙNG CHUNG cho toàn bộ project)
│   ├── modules/
│   │   ├── idea_generation.py       # Sinh domain/keyword/title
│   │   ├── outline_generation.py    # Sinh outline (~100KB – rất lớn)
│   │   ├── outline_prompt_bank.py   # Prompts cho outline (~71KB)
│   │   ├── knowledge_prompt_bank.py # Prompts cho knowledge base (~41KB)
│   │   ├── ingest_docs.py           # Embed & ingest vào Qdrant (~38KB)
│   │   ├── search_papers.py         # OpenAlex API
│   │   ├── search_journals.py       # Tìm journal
│   │   └── search_web.py            # SerpAPI
│   └── schemas/                     # outline.py, proposal.py, search_papers.py, search_web.py
│
├── write_reports/src/
│   ├── modules/
│   │   ├── write_graph.py           # LangGraph viết báo cáo (~40KB)
│   │   ├── write_prompt_bank.py     # Prompts viết (~125KB – RẤT LỚN)
│   │   ├── write_chat_graph.py      # Writer chatbot workflow
│   │   ├── seminar_graph_1/2/3.py   # 3 loại chuyên đề
│   │   ├── gen_slide_md.py          # Sinh slide (~53KB)
│   │   ├── references.py            # Quản lý references
│   │   └── translate.py             # Dịch báo cáo
│   └── schemas/                     # section.py, slide.py, translate.py
│
├── ai_chatbot/src/
│   ├── modules/
│   │   ├── chatbot.py               # Outer chatbot
│   │   ├── inner_chatbot.py         # Inner chatbot (research context)
│   │   ├── writer_chatbot.py        # Writer chatbot (~52KB)
│   │   ├── ai_in_doc.py             # AI in document (~26KB)
│   │   └── writer_prompt.py         # Prompts chatbot (~57KB)
│   └── schemas/
│
└── data_analysis/src/
    └── modules/
        ├── analyzer.py              # Phân tích dữ liệu chính (~23KB)
        ├── analyzer_graph.py        # LangGraph phân tích (~67KB)
        ├── proposed_method_v2.py    # Đề xuất phương pháp (~116KB)
        ├── prompt_bank.py           # Prompts phân tích (~94KB)
        └── utils.py                 # Utilities thống kê (~129KB – LỚN NHẤT)
```

---

## 5. Danh sách đầy đủ Events

### DocumentSetupEvent (topic: `document_setup_request`)
| Event | Mô tả | Timeout |
|---|---|---|
| `search_papers` | Tìm paper (OpenAlex) | 180s |
| `generate_domains` | Sinh domain nghiên cứu | 60s |
| `generate_subdomains` | Sinh subdomain | 60s |
| `generate_keywords` | Sinh từ khóa | 60s |
| `generate_titles` | Sinh tiêu đề bài nghiên cứu | 300s |
| `generate_title_description` | Mô tả tiêu đề (có thông tin) | 300s |
| `generate_title_description_no_info` | Mô tả tiêu đề (không có thông tin) | — |
| `mix_titles` | Gộp/biến thể tiêu đề | 300s |
| `generate_outline` | Sinh outline từ title | 1200s |
| `generate_outline_with_refs` | Sinh outline kèm references | — |
| `modify_outline` | Chỉnh sửa outline | — |
| `get_journals` | Lấy danh sách journal | 900s |
| `get_research_type` | Xác định loại nghiên cứu | 60s |
| `admin_documents` | Ingest tài liệu admin | 0s (no timeout) |
| `user_guide_documents` | Ingest tài liệu user guide | 0s |
| `user_documents` | Ingest tài liệu người dùng | 0s |
| `embed_user_documents` | Embed tài liệu người dùng | 0s |
| `delete_documents` | Xóa tài liệu | — |
| `update_user_refs` | Cập nhật references người dùng | — |
| `search_user_documents` | Tìm kiếm trong tài liệu người dùng | — |

### WriteEvent (topic: `write_section_request`)
| Event | Mô tả | Timeout |
|---|---|---|
| `write_content` | Viết nội dung báo cáo | 1500s |
| `analyzer` | Phân tích dữ liệu | 300s |
| `analyzer_tool` | Công cụ phân tích | 300s |
| `chuyen_de_1/2/3` | Viết 3 loại chuyên đề | 1500s |
| `gen_slides` | Sinh slide | 300s |
| `edit_report` | Chỉnh sửa báo cáo | — |
| `comment_data` | Comment dữ liệu | 300s |
| `propose_method` | Đề xuất phương pháp | 300s |
| `parsed_value` | Parse giá trị | 300s |
| `delete_report` | Xóa báo cáo | — |
| `translate_report` | Dịch báo cáo | — |
| `check_logic_data` | Kiểm tra logic dữ liệu | — |

### AIEvent (topic: `chatbot_request`)
| Event | Mô tả | Timeout |
|---|---|---|
| `outer_chatbot` | Chat chung | 120s |
| `inner_chatbot` | Chat trong context nghiên cứu | 120s |
| `writer_chatbot` | Hỗ trợ viết | 120s |
| `writer` | Viết qua chatbot | 1500s |

### EnhancementEvent (topic: `enhancement_request`)
| Event | Mô tả | Timeout |
|---|---|---|
| `ai_in_doc` | AI trong document | 120s |
| `get_suggestions` | Gợi ý nội dung | 120s |
| `enhance` | Nâng cao nội dung | 120s |
| `enhance_all` | Nâng cao toàn bộ | 120s |

---

## 6. Luồng xử lý chi tiết trong BaseAsyncListener

```python
# 1. Khởi động: init_singleton() → kết nối MongoDB, Redis, MinIO
# 2. AIOKafkaConsumer lắng nghe topic
# 3. Ingestor loop: nhận message → đẩy vào asyncio.Queue
# 4. Worker pool (100 concurrent workers): lấy từ queue → gọi _handle_record()
# 5. _handle_record() → parse event_type → dispatch tới module
# 6. Health check: PUT http://REDIS_HOST:4002/health-check/{document_id}
#    Status flow: PENDING(1) → PROCESSING(2) → FINISHED(3) / ERROR(4) / SUSPENDED(5)
# 7. Publish response về Kafka response topic
# 8. Lưu Redis key: DOCUMENT_REPORT:{document_id} = {timestamp} - {message_key}
```

**API Keys động**: Listener đọc từ Redis (`AI_KEY:{model_id}`) thay vì .env cứng → cho phép thay key runtime.

---

## 7. Singletons trong utils.py

| Singleton | Hàm khởi tạo | Dùng cho |
|---|---|---|
| `_MONGODB_CLIENT` | `get_mongodb_client()` | Lưu document data |
| `_REDIS_CLIENT` | `get_redis_client()` | Cache, health check status |
| `_REDIS_STACK_CLIENT` | `get_redis_stack_client()` | LangGraph Redis checkpointer |
| `_MINIO_CLIENT` | `get_minio_client()` | Object storage (MinIO SDK) |
| `S3_CLIENT` | `get_s3_client()` | Object storage (S3-compatible) |
| `CHECKPOINTER_MONGO` | `get_async_mongo_checkpoint()` | LangGraph MongoDB checkpointer |
| `CHECKPOINTER_REDIS` | `get_async_redis_checkpoint()` | LangGraph Redis checkpointer |
| `ADMIN_VECTOR_STORE` | `get_admin_vector_db(db_key)` | Qdrant vector search |

Tất cả được khởi tạo một lần trong `init_singleton()` khi app start.

---

## 8. MongoDB Collections

| Database | Collection | Mô tả |
|---|---|---|
| `BotGraph` | `BotCheckpoint` | LangGraph checkpoint states |
| `BotGraph` | `BotWrite` | LangGraph writes |
| `admin` | `outlines` | Outline data cho documents |

---

## 9. Config – AppSettings

Import từ: `from document_setup.src.configs.app import settings`

```python
settings.OPEN_AI_KEY        # OpenAI key (có thể override qua Redis AI_KEY:)
settings.GEMINI_KEY         # Gemini key
settings.SERP_KEY           # SerpAPI key
settings.KAFKA              # Kafka bootstrap servers
settings.REDIS / REDIS_HOST / REDIS_PORT / REDIS_PWD / REDIS_TTL
settings.REDIS_STACK        # Redis Stack URL (cho LangGraph)
settings.MINIO_ENDPOINT / MINIO_PORT / MINIO_ACCESS_KEY_ID / MINIO_SECRET_ACCESS_KEY
settings.MINIO_DOMAIN       # Cho S3 boto3 client
settings.QDRANT_URL         # Qdrant URL
settings.EMBEDDING_MODEL    # OpenAI embedding model name
settings.OPEN_ALEX_EMAIL / OPEN_ALEX_USER / OPEN_ALEX_KEY
settings.MONGO_CONNECTION_STRING
settings.TMP_FOLDER         # Thư mục file tạm
settings.LOCALLLM_BASE_URL  # Local LLM base URL
settings.API_KEY_ID         # Key cho health-check API
```

---

## 10. Local vs Production

| Yếu tố | Local | Production (chuẩn) |
|---|---|---|
| Topic suffix | `_local` (vd: `chatbot_request_local`) | Không có suffix |
| Entry point | `main_*_local.py` | `main.py` (4 listener) hoặc `main_*.py` (tách service) |
| Docker | `docker-compose.yaml` (app) + `docker-compose-infra.yaml` (hạ tầng local) | `docker-compose.yaml` / `docker-compose-<service>.yaml` |
| Deploy | `./deploy.sh all` trên máy dev | Jenkins CI/CD → server `192.168.3.140` |

> Compose dev cũ (`docker-compose-local.yaml`, `docker-compose-app.yaml`) đã được archive tại
> `backup/docker_obsolete/` và **không còn là chuẩn**. Smoke test mặc định dùng topic production;
> thêm `--local` nếu chạy entrypoint `main_*_local.py`.

---

## 11. Quy trình thêm Event mới (step-by-step)

**Ví dụ**: Thêm event `GENERATE_ABSTRACT` vào `DocumentSetupEvent`

```
Bước 1: event_type.py
  → class DocumentSetupEvent: thêm GENERATE_ABSTRACT = "generate_abstract"

Bước 2: error_type.py
  → class DocumentSetupError: thêm GENERATE_ABSTRACT = 629
  → ERROR_DICT: "generate_abstract": 629
  → TIME_OUT_DICT: "generate_abstract": 300   # timeout 5 phút
  → MAX_TOKENS_DICT: "generate_abstract": 2000

Bước 3: document_setup/src/schemas/
  → Tạo hoặc thêm Pydantic model cho request/response

Bước 4: document_setup/src/modules/
  → Tạo hàm/module xử lý logic (sử dụng LangGraph nếu phức tạp)
  → Thêm prompt vào outline_prompt_bank.py hoặc tạo *_prompt_bank.py mới

Bước 5: document_setup_listener.py
  → Thêm case trong hàm _handle_record():
    elif event_type == DocumentSetupEvent.GENERATE_ABSTRACT:
        result = await generate_abstract(...)
```

---

## 12. Pattern điển hình trong Listener

```python
# Trong *_listener.py – ví dụ điển hình của một event handler
async def _handle_record(self, record):
    value = json.loads(record.value)
    event_type = value["event_type"]
    document_id = value.get("document_id", "")
    
    # Đọc API keys từ Redis (dynamic key override)
    llm_key, db_key, search_key = await self.get_key(value.get("model_id", ""))
    
    # Tạo timer task để gửi health check định kỳ
    timeout = TIME_OUT_DICT.get(event_type, 300)
    timer_task = asyncio.create_task(
        self.task_timer(document_id, event_type, RESPONSE_TOPIC, timeout)
    )
    
    try:
        result = await some_module_function(value, llm_key)
        await self._finalize_task(
            timer_task, success=True, _id=document_id,
            producer=producer, topic=RESPONSE_TOPIC,
            message=result, event_type=event_type
        )
    except AIERROR as e:
        await self._finalize_task(
            timer_task, success=False, _id=document_id, ...
            message={"error_code": e.status_code, "error": str(e)}
        )
```

---

## 13. Điểm cần chú ý khi phát triển tiếp

### ⚠️ File lớn – KHÔNG đọc toàn bộ
| File | Size | Cách tiếp cận |
|---|---|---|
| `data_analysis/.../utils.py` | ~129KB | Dùng Fastcode `search_symbol` |
| `write_reports/.../write_prompt_bank.py` | ~125KB | Dùng Fastcode `search_symbol` |
| `data_analysis/.../proposed_method_v2.py` | ~116KB | Dùng Fastcode `get_call_chain` |
| `write_listener.py` | ~98KB | Dùng Fastcode `get_file_summary` |
| `document_setup_listener.py` | ~86KB | Dùng Fastcode `search_symbol` |

### 🔑 API Key cơ chế kép
- Mặc định: đọc từ `settings.OPEN_AI_KEY`
- Override: đọc từ Redis key `AI_KEY:{model_id}` → cho phép multi-tenant key

### 💾 LangGraph Checkpoint
- Chatbot dùng **Redis checkpointer** (`CHECKPOINTER_REDIS`)
- Write/Complex workflow dùng **MongoDB checkpointer** (`CHECKPOINTER_MONGO`)
- Cleanup sau mỗi task: `await self.clean_up(thread_id, document_id)`

### 🧹 Health Check Flow
```
PENDING → PROCESSING (heartbeat mỗi TTL/2 giây) → FINISHED / ERROR / SUSPENDED
```
Gọi HTTP PUT tới `http://REDIS_HOST:4002/health-check/{document_id}`

### 📁 File tạm
Dùng `settings.TMP_FOLDER` — **không dùng `/tmp` hardcode**.

### 🐳 Deployment
Mỗi service có `docker-compose-*.yaml` và `Jenkinsfile-*` riêng → có thể deploy độc lập.

---

## 14. Điểm cần refactor (technical debt)

| Vấn đề | File | Ưu tiên |
|---|---|---|
| File quá lớn (>100KB) | `write_listener.py`, `document_setup_listener.py` | Cao |
| Prompt bank quá lớn | `write_prompt_bank.py` (~125KB) | Trung bình |
| Chưa có test suite | Toàn bộ dự án | Cao |
| `REDIS_STACK` chưa có trong AppSettings mẫu | `utils.py` dùng `settings.REDIS_STACK` | Cần kiểm tra |

---

*File này được tạo tự động từ phân tích codebase. Cập nhật khi có thay đổi kiến trúc lớn.*
