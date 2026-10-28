# 🔁 PLAN — Nạp lại Gemini key mới & re-verify (sau khi đổi `.env`)

> **Mục tiêu:** nạp GEMINI_KEY mới vào runtime container qua Docker Compose, xác minh hoạt động bằng 1 lời gọi Gemini tối thiểu qua đúng luồng, không làm lộ key, không commit `.env`.
> **Phạm vi:** container app `autoresearching-ai-dev` (+ model-manager nếu đang chạy), Redis AI_KEY, 1 smoke test Gemini. Không đụng dữ liệu nghiệp vụ.
> **Nguyên tắc an toàn:** tuyệt đối KHÔNG in/log/commit giá trị key; mọi kiểm tra chỉ in `len`, `bool`, hash prefix ngắn.

---

## 1. Tiền kiểm (an toàn, read-only)

```bash
cd /home/hiubao/Projects/03_Work/Company/autoresearching

# .env có bị git theo dõi không? (phải là "bị ignore" mới an toàn)
git check-ignore -v .env
git status --porcelain | grep -E "^ M \.env|^M  \.env" && echo "CẢNH BÁO: .env đang staged" || echo "OK: .env không nằm trong thay đổi git"

# Sao lưu .env hiện tại vào vùng gitignore (không commit)
cp .env .cleanup_snapshot/env_backup_$(date +%Y%m%d_%H%M%S).bak
sha256sum .env | awk '{print "sha256(.env)=", $1}'   # chỉ in hash, KHÔNG in nội dung
```

| PASS | FAIL |
|---|---|
| `.env` bị ignore + hash lưu được | `.env` staged → **dừng**, không commit |

## 2. Nạp lại cấu hình vào container

App đọc `.env` qua `env_file` → phải `--force-recreate`:

```bash
docker compose -f docker-compose.yaml up -d --force-recreate
# model-manager nếu đang chạy (nó cũng đọc .env):
docker ps --filter name=autoresearching-model-manager --format "{{.Names}}" | grep -q . && \
  docker compose -f docker-compose.yaml up -d --force-recreate 2>/dev/null || true
```

> LiteLLM chỉ dùng `GROQ_API_KEY`, không cần recreate vì key Gemini không liên quan; để nguyên hạ tầng.

Chờ listener khởi động:

```bash
sleep 25
docker logs autoresearching-ai-dev 2>&1 | grep -c "Ingestor loop started"   # kỳ vọng 4
```

## 3. Xác minh env runtime (che giá trị)

```bash
docker exec autoresearching-ai-dev python - <<'PY'
from document_setup.src.configs.app import settings
k = (settings.GEMINI_KEY or "").strip()
print("GEMINI_KEY loaded:", "YES" if k else "NO", "| len =", len(k))
print("GEMINI_KEY prefix fingerprint:", (k[:3] + "..." + k[-3:]) if len(k) > 10 else "TOO_SHORT")
PY
```

| PASS | FAIL |
|---|---|
| `YES`, `len` phù hợp độ dài key mới, chỉ in fingerprint 3+3 | `NO`/`TOO_SHORT` → chưa nạp đúng, dừng |

## 4. Nạp key vào Redis (listener lấy key từ Redis, không từ process env)

`listener.get_key()` đọc `AI_KEY:{model_id}`. Seed lại từ `.env` mới:

```bash
docker exec autoresearching-ai-dev python scripts/seed_infra.py 2>&1 | tail -5

# Bổ sung đúng model đang dùng (gemini-3.5-flash-lite) — KHÔNG in key
docker exec -i autoresearching-ai-dev python - <<'PY'
import asyncio, json
from document_setup.src.configs.app import settings
from redis.asyncio import Redis
async def main():
    host = (settings.REDIS_HOST or "localhost").strip('"')
    pwd = (settings.REDIS_PWD or "").strip('"') or None
    r = Redis(host=host, port=settings.REDIS_PORT or 6379, password=pwd, decode_responses=True)
    key = (settings.GEMINI_KEY or "").strip('"')
    await r.set("AI_KEY:gemini-3.5-flash-lite", json.dumps(key))
    print("AI_KEY:gemini-3.5-flash-lite ->", "SET" if key else "EMPTY", "| len =", len(key))
    await r.aclose()
asyncio.run(main())
PY
```

## 5. Gọi Gemini tối thiểu qua đúng luồng (1 event nhẹ)

Dùng runner smoke tổng quát, model `gemini-3.5-flash-lite`:

```bash
docker exec -e SMOKE_MODEL_ID=gemini-3.5-flash-lite autoresearching-ai-dev \
  python scripts/smoke_event.py --group enhancement --event get_suggestions \
  --payload scripts/payloads/get_suggestions.json --timeout 180
```

Hoặc lời gọi trực tiếp tối thiểu (chỉ in tokens, không in nội dung key):

```bash
docker exec -e SMOKE_MODEL_ID=gemini-3.5-flash-lite autoresearching-ai-dev python - <<'PY'
import asyncio
from document_setup.src.configs.app import settings
from get_llm_response import get_llm, get_answer
async def main():
    llm = get_llm("gemini-3.5-flash-lite", settings.GEMINI_KEY, 100)
    err, ok, result, i, o = await get_answer("reverify", llm, "Nói 'OK' nếu nhận được tin nhắn này.")
    print("success:", ok, "| tokens:", i, o, "| preview:", result[:40])
asyncio.run(main())
PY
```

| PASS | FAIL |
|---|---|
| `success=True`, `error_code null`, có `suggested_paragraphs` hoặc `OK` | `401`/`403`/`429` → đọc log, xác định: key sai / hết quota / rate limit |

## 6. Đảm bảo key KHÔNG xuất hiện trong log

```bash
# Google key thường bắt đầu AIza — đếm trong log 10 phút gần nhất (kỳ vọng 0)
docker logs --since 10m autoresearching-ai-dev 2>&1 | grep -c "AIza" || true
docker logs --since 10m autoresearching-ai-dev 2>&1 | grep -c "GEMINI_KEY" || true
```

| PASS | FAIL |
|---|---|
| cả 2 count = 0 | > 0 → rò rỉ key trong log, báo ngay |

## 7. Đối chiếu trước/sau

| Hạng mục | Trước | Sau | Ghi |
|---|---|---|---|
| `sha256(.env)` | lưu bước 1 | hash mới sau sửa | dùng để rollback |
| GEMINI_KEY len | giá trị cũ | giá trị mới | chỉ so len, không so nội dung |
| Listener | 4 | 4 | — |
| Smoke Gemini | n/a | pass/fail | bằng chứng |

## 8. Rollback nếu lỗi

```bash
cp .cleanup_snapshot/env_backup_<ts>.bak .env
docker compose -f docker-compose.yaml up -d --force-recreate
docker exec autoresearching-ai-dev python scripts/seed_infra.py
```

## 9. Cập nhật handoff

- Nếu key mới PASS: ghi dòng mới vào [`CONTINUE_HERE.md`](CONTINUE_HERE.md) (mục TL;DR): `GEMINI_KEY đã nạp lại + smoke get_suggestions PASS ngày ...`.
- Nếu FAIL: ghi rõ blocker + log đã lưu ở `.cleanup_snapshot/`.

## 10. Tiêu chí nghiệm thu

1. Container app Up + 4 listener.
2. `GEMINI_KEY loaded: YES`, len > 0, **không in giá trị**.
3. Redis có `AI_KEY:gemini-3.5-flash-lite` SET.
4. 1 smoke/LLM call `success=True`, không `401/403`.
5. Log không chứa `AIza`/`GEMINI_KEY`.
6. `git status` không có `.env` staged; không commit `.env`.
7. `CONTINUE_HERE.md` đã cập nhật trạng thái mới.
