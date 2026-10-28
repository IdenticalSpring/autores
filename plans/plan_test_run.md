# 🧪 KẾ HOẠCH TEST PHIÊN BẢN GỐC — autoresearching (đã sửa, chưa xác nhận ổn định)

> **Mục tiêu:** phát hiện bug/lỗi **trước khi** xác nhận bản gốc ổn định.
> **Kiến trúc chuẩn:** [`docker-compose.yaml`](../docker-compose.yaml:1) (1 container `autoresearching-ai-dev`, 4 listener, topic production) + [`docker-compose-infra.yaml`](../docker-compose-infra.yaml:1) (Kafka, Redis, MongoDB, Qdrant, MinIO, LiteLLM, Kafka UI).
> **Điều kiện hiện tại:** `.env` đã có API key; `deploy.sh` hỗ trợ `build/up/status/logs/health`; `test/` có pytest; `scripts/` có smoke test.

---

## 0. Nguyên tắc thu thập log & xử lý bug

| Việc | Cách làm |
|---|---|
| Log runtime | `docker logs --tail 200 -f autoresearching-ai-dev > logs_run_<date>.log 2>&1` |
| Log infra | `docker logs autoresearching-kafka` / `-mongodb` / `-redis` khi có lỗi kết nối |
| Log test | chạy smoke với `2>&1 | tee smoke_<name>_<date>.log` |
| Khi gặp bug | **dừng bước đó**, lưu log + snapshot `docker ps -a`, không đoán mò, phân loại lỗi: infra / env / code / contract |
| Xử lý an toàn | chỉ sửa nếu xác định được root cause; nếu blocked → ghi rõ blocker, không thay đổi code lan man |
| Lưu trữ | mọi log đặt trong `.cleanup_snapshot/` (đã gitignore) hoặc `logs/` |

---

## 1. Pre-flight (chạy TRƯỚC khi build/up)

### 1.1. Git sạch & đúng commit

```bash
git status --porcelain
git log --oneline -1
```

| PASS | FAIL |
|---|---|
| Output rỗng (hoặc chỉ file log gitignored) + HEAD = commit chuẩn (hiện `20e813a`) | Có file sửa dở → **dừng**, `git stash` hoặc xử lý trước |

### 1.2. Kiểm tra `.env` (đã điền key)

```bash
grep -nE "^(KAFKA|REDIS_HOST|REDIS_PORT|REDIS_PWD|MONGO_CONNECTION_STRING|QDRANT_URL|MINIO_ENDPOINT|MINIO_ACCESS_KEY_ID|MINIO_SECRET_ACCESS_KEY|MINIO_BUCKET|GEMINI_KEY|OPEN_AI_KEY|GROQ_API_KEY|SERP_KEY|OPEN_ALEX_EMAIL|LOCALLLM_BASE_URL)" .env
```

| PASS | FAIL |
|---|---|
| KAFKA/Redis/Mongo/Qdrant/MinIO đúng localhost; **ít nhất 1 trong `GEMINI_KEY`/`OPEN_AI_KEY`/`GROQ_API_KEY` không rỗng** | Key trống → không thể chạy e2e; vẫn chạy được test không cần LLM (pytest + round-trip) |

### 1.3. Kiểm tra port bị chiếm (đặc biệt 8000 của LiteLLM)

```bash
for p in 9092 6379 27017 6333 9000 9001 8080 8000; do
  timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$p" 2>/dev/null && echo "port $p: BUSY" || echo "port $p: FREE";
done
docker ps --format "{{.Names}}\t{{.Ports}}" | grep -E "8000|9092" || true
```

| PASS | FAIL | Xử lý |
|---|---|---|
| 8000 FREE (để LiteLLM chạy) | 8000 BUSY (hiện do `ada_app` project khác) | Không dừng project khác; ghi nhận "LiteLLM bị skip", e2e dùng model Gemini/OpenAI trực tiếp thay vì `localhost` |

### 1.4. Trạng thái hạ tầng & container hiện có

```bash
docker ps -a --filter "name=autoresearching" --format "table {{.Names}}\t{{.Status}}"
docker volume ls | grep autoresearching || true
```

| PASS | FAIL |
|---|---|
| Biết rõ container nào đang Up / Exited / Restarting | Có container `Restarting` → ghi log trước khi xử lý |

---

## 2. Build & khởi động

### 2.1. Build image

```bash
./deploy.sh build
```

| PASS | FAIL |
|---|---|
| Kết thúc `Successfully tagged autoresearching-ai-dev:latest`, `docker images` có tag | Build fail → xem lỗi pip/apt, **dừng**, không up |

### 2.2. Up hạ tầng

```bash
./deploy.sh infra-up
sleep 20
docker compose -f docker-compose-infra.yaml ps
```

| PASS | FAIL |
|---|---|
| Kafka/Redis/Mongo/Qdrant/MinIO `healthy` (LiteLLM có thể bị skip do port) | Service nào `Exited/Restarting` → `docker logs <svc>` trước khi tiếp tục |

### 2.3. Up app (container 4 listener)

```bash
./deploy.sh up
sleep 25
./deploy.sh status
```

| PASS | FAIL |
|---|---|
| `autoresearching-ai-dev  Up` (không Restarting) | Restarting/Exited → `docker logs --tail 100 autoresearching-ai-dev` |

---

## 3. Health check (mức cơ bản)

```bash
./deploy.sh health
docker logs autoresearching-ai-dev 2>&1 | grep -c "Ingestor loop started"
```

| Hạng mục | PASS | FAIL |
|---|---|---|
| Container | `Up` | không Up |
| Listener count | **= 4** | < 4 → tìm traceback trong log |
| Port 9092/6379/27017/6333/9000 | READY | FAIL → infra chưa lên |

Log khởi động phải chứa: `Connected to MongoDB`, `Connected to Redis`, `Connected to Minio`, 4 dòng subscribe tới `document_setup_request`, `write_section_request`, `chatbot_request`, `enhancement_request`.

---

## 4. Kafka: topics & consumer groups

```bash
docker exec autoresearching-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | sort
docker exec autoresearching-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

| PASS | FAIL |
|---|---|
| Đủ 16 topic (8 cặp request/response + bản `_local`) | Thiếu topic production → restart listener để `set_num_partitions` tạo lại |
| 3 group `document_setup_05_20_2025`, `write_section_05_20_2025`, `chatbot_09_06_2025` | Thiếu group → listener chưa join; xem log aiokafka |

> Lưu ý: `enhance` dùng chung group `chatbot_09_06_2025` với `chat` (khác topic nên chạy được) — ghi nhận là điểm cần theo dõi.

---

## 5. pytest (unit + regression)

```bash
docker exec autoresearching-ai-dev python -m pytest -q test/ 2>&1 | tee .cleanup_snapshot/pytest_$(date +%Y%m%d).log | tail -15
```

| PASS | FAIL |
|---|---|
| `72 passed` (hoặc số pass tăng, 0 failed) | Có test fail → `docker exec ... pytest -q <test> -x` để cô lập; KHÔNG xác nhận ổn định khi còn fail |

---

## 6. Smoke test từng service (topic production)

Các script mặc định dùng **topic production**; thêm `--local` nếu cố tình test topic `_local`. `model_id` mặc định `localhost` (LiteLLM) — **khi LiteLLM bị skip, truyền `SMOKE_MODEL_ID`** tới model có key thật.

```bash
# 6.1 Chatbot (outer_chatbot) — nhẹ, không cần file
SMOKE_MODEL_ID=<model thật> docker exec autoresearching-ai-dev python scripts/test_kafka_chatbot.py

# 6.2 Enhancement (ai_in_doc)
SMOKE_MODEL_ID=<model thật> docker exec autoresearching-ai-dev python scripts/test_kafka_enhance.py

# 6.3 Document setup: idea_gen (generate_domains)
SMOKE_MODEL_ID=<model thật> docker exec autoresearching-ai-dev python scripts/test_kafka_idea_gen.py

# 6.4 Document setup: search_papers (cần OpenAlex/API key)
SMOKE_MODEL_ID=<model thật> docker exec autoresearching-ai-dev python scripts/test_kafka_search_papers.py

# 6.5 Write: gen_slides (nặng, tùy chọn — chỉ chạy nếu 6.1–6.4 pass)
SMOKE_MODEL_ID=<model thật> docker exec autoresearching-ai-dev python scripts/test_kafka_write.py
```

| PASS | FAIL |
|---|---|
| Nhận response đúng `document_id/session_id`, có `event_type`, **không treo tới timeout** | Timeout → xem listener log; `error_code` → phân loại: `401` = key/model sai, `5xx` = code/proxy, `4xx` khác = payload |

### 6.6. Smoke `admin_documents` (bắt buộc nếu có file mẫu)

```bash
docker exec autoresearching-ai-dev python scripts/smoke_admin_documents.py --timeout 300
```

| PASS | FAIL |
|---|---|
| Nhận response có đủ `event_type,user_id,document_id,session_id`; `processed_docs` không rỗng khi thành công | Timeout / thiếu identifier → **bug nghiêm trọng**, dừng xác nhận |

> Nếu chưa có `autoresearching/sample/sample.pdf` trong MinIO: upload thử 1 PDF nhỏ rồi mới chạy, hoặc đánh dấu bước này là SKIP có ghi chú.

---

## 7. Kiểm tra luồng end-to-end với API key mới (bắt buộc)

### 7.1. Seed API key vào Redis

```bash
# key được đọc từ .env và nạp vào Redis dạng AI_KEY:{provider} (JSON)
./deploy.sh status   # đảm bảo Redis đang chạy trước
docker exec autoresearching-ai-dev python scripts/seed_infra.py 2>&1 | tail -30
```

| PASS | FAIL |
|---|---|
| Log seed thành công, không lỗi Redis/MinIO | Lỗi → xem `docker logs autoresearching-redis`, kiểm tra lại `REDIS_PWD` |

### 7.2. Xác minh model + key thật

```bash
docker exec autoresearching-ai-dev python -c "from model_manager.service import model_service; import asyncio; print(asyncio.run(model_service.list_models()))"
```

Hoặc đơn giản hơn: chạy 1 smoke test với `SMOKE_MODEL_ID=<model thật>` và kiểm tra **response không còn `error_code: 401`**.

| PASS | FAIL |
|---|---|
| Response có nội dung thật hoặc lỗi nghiệp vụ khác 401 (vd `603` payload sai) | Vẫn `401` → key/provider chưa khớp, không chuyển sang xác nhận ổn định |

### 7.3. Chuỗi e2e gợi ý (theo đúng luồng người dùng)

| Thứ tự | Event | Script | Kỳ vọng |
|---|---|---|---|
| 1 | `generate_domains` | `test_kafka_idea_gen.py` | trả danh sách lĩnh vực |
| 2 | `search_papers` | `test_kafka_search_papers.py` | trả bài báo OpenAlex |
| 3 | `outer_chatbot` | `test_kafka_chatbot.py` | trả câu trả lời |
| 4 | `ai_in_doc` | `test_kafka_enhance.py` | trả đề xuất sửa văn bản |
| 5 | `admin_documents` | `smoke_admin_documents.py` | trả `processed_docs` |

Mỗi bước ghi: request topic, response topic, thời gian phản hồi, `error_code` (nếu có).

---

## 8. Tiêu chí pass/fail tổng hợp để "xác nhận ổn định"

| # | Tiêu chí | PASS |
|---|---|---|
| C1 | `deploy.sh health`: container Up + 4 listener | ✅ |
| C2 | 16 Kafka topic tồn tại, 3 consumer group active | ✅ |
| C3 | `pytest test/` không fail | ✅ |
| C4 | 3/5 smoke test cơ bản (chatbot, enhance, idea_gen) nhận response đúng identifier | ✅ |
| C5 | Ít nhất 1 smoke dùng API key thật trả **không phải 401** | ✅ |
| C6 | Không có traceback `Unhandled exception` mới trong log listener suốt quá trình test | ✅ |
| C7 | `git status` sạch, không phát sinh thay đổi code ngoài dự kiến | ✅ |

**Bất kỳ tiêu chí C3/C4/C5 FAIL ⇒ KHÔNG xác nhận ổn định.**

---

## 9. Quy trình khi phát hiện bug

1. **Ghi nhận**: lưu log, payload request, response, thời điểm, `docker ps -a`.
2. **Phân loại**:
   - `infra`: container Restarting, port conflict, kết nối Redis/Mongo/Kafka fail.
   - `env`: key sai, biến thiếu, `LOCALLLM_BASE_URL` sai.
   - `code`: traceback Python, import error, lỗi logic.
   - `contract`: thiếu identifier, sai schema response, sai topic.
3. **Khoanh vùng**: chạy lại 1 test đơn lẻ với `-x`; bật `--local` để tách biệt topic nếu cần.
4. **Sửa tối thiểu** (chỉ khi root cause rõ) hoặc **dừng và báo blocker** nếu cần ý kiến BE/DevOps.
5. **Sau sửa**: build lại image → restart → chạy lại đúng test đã fail → ghi kết quả vào cùng file log.

## 10. Danh sách artifact cần thu thập

| Artifact | Nơi lưu |
|---|---|
| Log build | `.cleanup_snapshot/build.log` |
| Log listener | `.cleanup_snapshot/listener_<date>.log` |
| Log pytest | `.cleanup_snapshot/pytest_<date>.log` |
| Log từng smoke | `.cleanup_snapshot/smoke_<event>_<date>.log` |
| Snapshot container trước/sau | `.cleanup_snapshot/docker_ps_*.txt` |
| Kết quả e2e | bảng ở mục 7.3 (điền trực tiếp) |
