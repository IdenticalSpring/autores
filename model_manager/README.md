# 🤖 Model Manager Web UI

Giao diện Web trực quan và REST API quản lý danh sách Model AI & API Keys lưu trữ trên Redis.

---

## 🌟 Tính Năng Chính
1. **Quản lý Model AI động**: Thêm, cập nhật, xóa model (Google Gemini, OpenAI, Groq, Local LLM) trực tiếp trên Web UI mà không cần can thiệp code hay restart container trên VPS.
2. **Nạp thẳng vào Redis**: Lưu key theo chuẩn `AI_KEY:{model_id}` (định dạng `json.dumps(key)`), tương thích 100% với `main_*.py` và `litellm`.
3. **Bảo mật**:
   - Masking API keys (`AIzaSy...****`) trên toàn bộ giao diện và REST responses.
   - Xác thực bảo mật bằng Bearer token HMAC dựa trên `ADMIN_PASSWORD`.
   - Backup tự động danh sách keys vào file `tmp/models_backup.json`.
4. **Kiểm tra kết nối Live**: Nút "Kiểm tra" trực tiếp đo độ trễ (latency ms) và xác nhận API key hoạt động với nhà cung cấp.

---

## 🚀 Truy Cập Web UI
- **URL**: `http://<IP_VPS_HOẶC_LOCALHOST>:8088`
- **Mật khẩu quản trị**: Lấy từ biến `ADMIN_PASSWORD` trong file `.env` (mặc định: `admin123`).

---

## 📡 REST API Endpoints
- `POST /api/auth/login`: Đăng nhập bằng mật khẩu quản trị, nhận Bearer token.
- `GET /api/models`: Lấy danh sách model đã cấu hình.
- `POST /api/models`: Thêm hoặc cập nhật model (`model_id`, `key_type`, `custom_key`).
- `DELETE /api/models/{model_id}`: Xóa model khỏi Redis.
- `POST /api/models/{model_id}/test`: Kiểm tra kết nối thực tế tới AI Provider.
- `GET /api/system/keys`: Lấy danh sách các key hệ thống đang có trong file `.env`.
