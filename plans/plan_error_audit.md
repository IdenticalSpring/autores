# 🔎 KẾ HOẠCH AUDIT LỖI — container `autoresearching-ai-dev`

> **Mục tiêu:** kiểm tra lại toàn bộ lỗi hiện tại của hệ thống đang chạy trong container, phân biệt **lỗi thật cần fix** vs **nhầm lẫn khi test**, rồi ghi tổng kết vào `.cleanup_snapshot/error_audit.md`.
> **Phạm vi:** chỉ đọc (read-only) + kiểm tra kết nối. **Không sửa code, không commit** trong bước audit.
> **Điều kiện tiên quyết:** container `autoresearching-ai-dev` đang Up; `./deploy.sh` tồn tại; `.cleanup_snapshot/` có log smoke từ lần test trước.

---

## 1. Các bước thực hiện (theo thứ tự)

| # | Bước | Lệnh | Kết quả cần ghi lại |
|---|---|---|---|
| A1 | Git chưa commit | `git status --porcelain`; `git diff --stat`; `git diff` (nếu có) | Danh sách file thay đổi + nội dung diff (để loại trừ "lỗi do code đang sửa dở") |
| A2 | Đọc log smoke đã có | `ls -1 .cleanup_snapshot/smoke_*.log`; đọc `smoke_admin_documents_*.log`, `smoke_write_*.log`, `smoke_chatbot_*.log`, `smoke_enhance_*.log`, `smoke_idea_gen_*.log`, `smoke_search_papers_*.log` | Trích các dòng `ERROR`, `error_code`, `FAIL`, `timeout` |
| A3 | Lọc log container 30 phút | `docker logs --since 30m autoresearching-ai-dev` rồi grep các mẫu: `Unhandled exception`, `Traceback (most recent call last)`, `Task failed`, `AIERROR`, `ERROR`, `list_buckets`, `list_objects_v2`, `NoSuchBucket`, `AccessDenied`, `timeout`, `Connect call failed`, `Status: 600` | Bảng đếm số lần xuất hiện theo mẫu |
| A4 | Health container | `./deploy.sh health` | Container status + listener count (kỳ vọng 4) + port READY |
| A5 | Kiểm tra S3/MinIO bằng code | `docker exec -i autoresearching-ai-dev python - <<'PY'` dùng `utils.get_s3_client()`: `list_buckets()`, `list_objects_v2` cho `autoresearching`, `documents`, `users`, `analysis` | Danh sách bucket + số object + lỗi (nếu có) |
| A6 | Đối chiếu log ↔ code | Đọc đúng file/hàm nghi vấn (mục 2) | Kết luận: lỗi thật / nhầm lẫn / không phải lỗi / cần điều tra |
| A7 | Tổng hợp | — | Ghi `.cleanup_snapshot/error_audit.md` đúng cấu trúc mục 3 |

**Nguyên tắc khi chạy:** lệnh chỉ đọc; nếu gặp lỗi chặn (container không Up, `docker exec` fail) → dừng và ghi rõ blocker, không tự ý sửa.

---

## 2. Các nghi vấn đã biết cần đối chiếu (từ lần test trước)

| # | Triệu chứng | Nguồn | Điểm code cần đọc | Giả thuyết ban đầu |
|---|---|---|---|---|
| S1 | `gen_slides` trả `error_code 634`, log `UnsupportedProtocol: Request URL is missing an 'http://' or 'https://' protocol` | `smoke_write_*.log`, docker logs | [`write_reports/src/modules/gen_slide_md.py:655`](../write_reports/src/modules/gen_slide_md.py:655) — `fetch_html_content()`, dòng 661 `client.get(state["raw_report"])` | `raw_report` là **đường dẫn MinIO `bucket/key`** chứ không phải URL http ⇒ cần kiểm tra nguồn `raw_report` trong [`write_listener.py`](../write_listener.py:1) và dữ liệu Mongo |
| S2 | `Status: 600, Message: All connection attempts failed` (lặp 2 lần) | docker logs | [`utils.py:305`](../utils.py:305) — `send_health_check()`, dòng 318 `http://{REDIS_HOST}:4002/health-check/{id}` | Port **4002 = BE gateway**, local không có ⇒ **không phải lỗi code**; trên VPS phải có BE |
| S3 | `search_papers` script timeout 60s, log `429 ResourceExhausted quota 15` | `smoke_search_papers_*.log` | [`scripts/test_kafka_search_papers.py`](../scripts/test_kafka_search_papers.py:89) — vòng chờ 60s | Giới hạn **Gemini free tier 15 req/phút** + timeout cứng 60s ⇒ nâng timeout, không phải bug logic |
| S4 | Nghi vấn MinIO/S3 (`list_buckets`, `list_objects_v2`, `NoSuchBucket`, `AccessDenied`) | cần grep A3 | [`utils.py:209`](../utils.py:209) — `get_s3_client()` dùng `settings.MINIO_DOMAIN`; [`document_setup/src/modules/minio_path.py:11`](../document_setup/src/modules/minio_path.py:11) — `parse_minio_path()` | Có thể do **gọi S3 client sai API** (truyền positional arg vào boto3) hoặc **bucket chưa tạo**; phải verify bằng A5 |
| S5 | `list_buckets`/`list_objects_v2` lỗi trong script kiểm tra tay | lần audit trước | `get_s3_client()` trả **boto3 client**, không phải MinIO client | Nhầm lẫn API (`list_objects(bucket_name=...)` của MinIO ≠ `list_objects_v2(Bucket=...)` của boto3) — **không phải lỗi code** |
| S6 | `enhance`/`chat` cùng `group_id chatbot_09_06_2025` | [`main.py:33`](../main.py:33) | — | Không lỗi hiện tại (khác topic), cần theo dõi |
| S7 | `concurrency = 100` × 4 listener | [`listener.py:24`](../listener.py:24) | — | Rủi ro tài nguyên, không phải lỗi log |
| S8 | Group `smoke_test_enhance_092217` còn sót | Kafka | — | Rác test, không phải lỗi |

---

## 3. Cấu trúc bắt buộc của `.cleanup_snapshot/error_audit.md`

1. **Tóm tắt tình trạng & phạm vi kiểm tra** — thời điểm, container, commit, số mẫu log đã quét.
2. **Bảng tổng hợp lỗi** — cột: `STT | Lỗi/Triệu chứng | Nguồn phát hiện (log/file/dòng) | Phân loại | Mức độ ưu tiên | File & hàm liên quan | Phương pháp fix đề xuất | Ghi chú`.
   - Phân loại chỉ dùng 4 nhãn: `Lỗi thật cần fix` / `Nhầm lẫn` / `Không phải lỗi` / `Cần điều tra thêm`.
   - Mức độ: `P0 (chặn)` / `P1 (cao)` / `P2 (trung bình)` / `P3 (thấp)`.
3. **Phân tích chi tiết từng lỗi thật cần fix** — nguyên nhân gốc, file, hàm, hướng sửa + đoạn code minh họa, test case kiểm tra sau fix.
4. **Danh sách lỗi do nhầm lẫn / không cần fix** — kèm lý do (sai endpoint, sai tên bucket, thiếu env, timeout tạm thời, log cũ).
5. **Đề xuất thứ tự fix & rủi ro** — thứ tự P0→P3, rủi ro khi sửa, cách rollback.

---

## 4. Tiêu chí phân loại (dùng thống nhất)

| Nhãn | Điều kiện |
|---|---|
| **Lỗi thật cần fix** | Lặp lại được, do code/logic/schema; tái hiện bằng test data hợp lệ; gây sai contract hoặc crash |
| **Nhầm lẫn** | Do cách test/kiểm tra sai (payload thiếu field, gọi sai API client, dùng sai topic `_local`, sai model_id) |
| **Không phải lỗi** | Hành vi đúng thiết kế: lỗi nghiệp vụ có mã lỗi, log cảnh báo của thư viện, endpoint ngoài phạm vi (BE chưa chạy) |
| **Cần điều tra thêm** | Có dấu hiệu bất thường nhưng chưa đủ bằng chứng/không tái hiện được |

---

## 5. Kiểm tra S3/MinIO chuẩn (A5) — mẫu lệnh

```bash
docker exec -i autoresearching-ai-dev python - <<'PY'
from utils import get_s3_client
s3 = get_s3_client()
print("buckets:", [b["Name"] for b in s3.list_buckets().get("Buckets", [])])
for b in ("autoresearching", "documents", "users", "analysis"):
    r = s3.list_objects_v2(Bucket=b, MaxKeys=20)
    print(b, r.get("KeyCount", 0), [o["Key"] for o in r.get("Contents", [])][:8])
PY
```

**Kỳ vọng:** 4 bucket tồn tại; `autoresearching` có ít nhất `sample/sample.pdf` (đã upload ở lần test trước).
**Nếu `NoSuchBucket`:** bucket chưa tạo → chạy `seed_infra.py` (không phải bug code).
**Nếu `AccessDenied`/`SignatureDoesNotMatch`:** sai `MINIO_*` trong `.env` → lỗi cấu hình, không phải code.

---

## 6. Mẫu lệnh lọc log (A3)

```bash
docker logs --since 30m autoresearching-ai-dev 2>&1 > .cleanup_snapshot/container_log_30m.txt

for pat in "Unhandled exception" "Traceback (most recent call last)" "Task failed" "AIERROR" \
           "list_buckets" "list_objects_v2" "NoSuchBucket" "AccessDenied" "timeout" "Status: 600"; do
  printf "%-42s %s\n" "$pat" "$(grep -c "$pat" .cleanup_snapshot/container_log_30m.txt)"
done

grep -nE "ERROR" .cleanup_snapshot/container_log_30m.txt | tail -40
```

**Lưu ý:** `Traceback` của ca lỗi đã biết (gen_slides 634) sẽ xuất hiện — phải gắn nhãn tương ứng chứ không tính là lỗi mới.

---

## 7. Rủi ro & giới hạn của audit

| Rủi ro | Giảm thiểu |
|---|---|
| Log 30 phút có thể đã bị rotate/mất ca lỗi cũ | Kết hợp log smoke trong `.cleanup_snapshot/` |
| `docker logs` rất lớn (hàng chục nghìn dòng) | Ghi ra file rồi grep, không in toàn bộ |
| Dễ nhầm "lỗi nghiệp vụ có mã lỗi" là bug | Dùng tiêu chí mục 4, đối chiếu `error_type.py` |
| Kết luận sai do test data | Mọi kết luận phải nêu rõ điều kiện tái hiện |

---

## 8. Đầu ra

| File | Nội dung |
|---|---|
| `.cleanup_snapshot/container_log_30m.txt` | Raw log 30 phút (bằng chứng) |
| `.cleanup_snapshot/s3_check.txt` | Kết quả kiểm tra bucket/object |
| `.cleanup_snapshot/error_audit.md` | **Báo cáo tổng kết theo cấu trúc mục 3** |
