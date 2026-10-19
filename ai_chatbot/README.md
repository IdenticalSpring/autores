# AI Chatbot Module

> Module AI Chatbot cung cấp 4 tính năng trò chuyện: Chatbot tổng quát (outer), Chatbot trong ngữ cảnh nghiên cứu (inner), Writer Chatbot hỗ trợ lập kế hoạch viết, và Writer Agent viết báo cáo tự động qua hội thoại.

## Folder Structure

```
.
├── README.md
└── src/
    ├── configs/
    │   └── app.py
    ├── modules/
    │   ├── chatbot.py             # Outer & Inner chatbot (Q&A + RAG)
    │   ├── inner_chatbot.py       # Inner chatbot logic (trong ngữ cảnh document)
    │   ├── writer_chatbot.py      # Writer chatbot (LangGraph workflow lập kế hoạch)
    │   ├── ai_in_doc.py           # AI tương tác trong document (dùng bởi Enhancement)
    │   ├── writer_prompt.py       # Prompt templates (~57KB)
    │   └── utils.py               # Shared utilities
    └── schemas/
```

## Kafka Topics

| Hướng | Topic (Local) | Topic (Production) |
|---|---|---|
| Request | `chatbot_request_local` | `chatbot_request` |
| Response | `chatbot_response_local` | `chatbot_response` |

---

## Events

### 1. Outer Chatbot (`outer_chatbot`)

Chatbot hỏi đáp tổng quát về nghiên cứu khoa học. Không cần ngữ cảnh document cụ thể. Hỗ trợ RAG từ knowledge base hệ thống và tùy chọn tìm kiếm web.

**Input Request:**

```json
{
  "event_type": "outer_chatbot",
  "model_id": "localhost",
  "user_id": "user_01",
  "conv_id": "conv_abc_123",
  "language": "Vietnamese",
  "question": "Phương pháp PLS-SEM có ưu điểm gì so với CB-SEM?",
  "chat_history": [
    {"role": "user", "content": "Xin chào AI"},
    {"role": "assistant", "content": "Chào bạn! Tôi có thể giúp gì?"}
  ],
  "short_answer": false,
  "direct_answer": false,
  "use_web_search_chatbot": false
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_type` | `str` | ✅ | Luôn là `"outer_chatbot"` |
| `model_id` | `str` | ✅ | Tên model LLM (`"localhost"`, `"gpt-4o"`, `"gemini-2.5-flash"`) |
| `user_id` | `str` | ✅ | ID người dùng |
| `conv_id` | `str` | ✅ | ID cuộc hội thoại (dùng để nhận diện kết quả trả về) |
| `language` | `str` | ✅ | Ngôn ngữ (`"Vietnamese"`, `"English"`) |
| `question` | `str` | ✅ | Câu hỏi của người dùng |
| `chat_history` | `list[dict]` | ✅ | Lịch sử hội thoại (`role` + `content`) |
| `short_answer` | `bool` | ❌ | Trả lời ngắn gọn (mặc định `false`) |
| `direct_answer` | `bool` | ❌ | Trả lời trực tiếp không qua RAG (mặc định `false`) |
| `use_web_search_chatbot` | `bool` | ❌ | Bật tìm kiếm web bổ sung (mặc định `false`) |

**Output Response:**

```json
{
  "event_type": "outer_chatbot",
  "model_id": "localhost",
  "conv_id": "conv_abc_123",
  "user_id": "user_01",
  "response": "PLS-SEM (Partial Least Squares SEM) có nhiều ưu điểm nổi bật: ...",
  "input_tokens": 320,
  "output_tokens": 450,
  "embed_tokens": 50,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0,
  "functionality_error": false
}
```

| Field | Type | Mô tả |
|---|---|---|
| `response` | `str` | Nội dung câu trả lời AI |
| `input_tokens` | `int` | Số token đầu vào |
| `output_tokens` | `int` | Số token đầu ra |
| `embed_tokens` | `int` | Số token embedding RAG |
| `embed_model` | `str` | Model embedding đã dùng |
| `web_search_call` | `int` | Số lần gọi web search |
| `functionality_error` | `bool` | `true` nếu chức năng không được hỗ trợ |

---

### 2. Inner Chatbot (`inner_chatbot`)

Chatbot hỏi đáp trong ngữ cảnh một dự án nghiên cứu cụ thể. Có quyền truy cập vào tài liệu tham khảo đã được vector hóa của dự án đó (RAG theo `document_id`).

**Input Request:**

```json
{
  "event_type": "inner_chatbot",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_project_01",
  "conv_id": "conv_inner_456",
  "language": "Vietnamese",
  "question": "Theo các tài liệu tham khảo đã tải lên, mô hình TAM có điểm gì hạn chế?",
  "chat_history": [],
  "short_answer": false,
  "direct_answer": false,
  "use_web_search_chatbot": false
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_type` | `str` | ✅ | Luôn là `"inner_chatbot"` |
| `model_id` | `str` | ✅ | Tên model LLM |
| `user_id` | `str` | ✅ | ID người dùng |
| `document_id` | `str` | ✅ | ID dự án nghiên cứu (dùng để truy vấn RAG riêng) |
| `conv_id` | `str` | ✅ | ID cuộc hội thoại |
| `language` | `str` | ✅ | Ngôn ngữ |
| `question` | `str` | ✅ | Câu hỏi |
| `chat_history` | `list[dict]` | ✅ | Lịch sử hội thoại |
| `short_answer` | `bool` | ❌ | Trả lời ngắn gọn (mặc định `false`) |
| `direct_answer` | `bool` | ❌ | Trả lời trực tiếp (mặc định `false`) |
| `use_web_search_chatbot` | `bool` | ❌ | Bật web search (mặc định `false`) |

**Output Response:**

```json
{
  "event_type": "inner_chatbot",
  "model_id": "localhost",
  "conv_id": "conv_inner_456",
  "document_id": "doc_project_01",
  "user_id": "user_01",
  "response": "Dựa theo các tài liệu bạn đã tải lên, mô hình TAM (Technology Acceptance Model) có một số hạn chế...",
  "input_tokens": 500,
  "output_tokens": 600,
  "embed_tokens": 120,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

---

### 3. Writer Chatbot (`writer_chatbot`)

Chatbot dạng multi-turn conversation dẫn dắt người dùng qua quy trình lập kế hoạch viết nghiên cứu (xác định mục tiêu, biến số, giả thuyết, phương pháp). Sử dụng LangGraph workflow với cơ chế interrupt/resume.

**Input Request (Lần đầu):**

```json
{
  "event_type": "writer_chatbot",
  "model_id": "localhost",
  "user_id": "user_01",
  "conv_id": "conv_writer_789",
  "language": "Vietnamese",
  "question": "Tôi muốn viết luận văn về tác động của livestream đến quyết định mua sắm Gen Z",
  "resume": false,
  "short_answer": false,
  "use_web_search_chatbot": false,
  "template_key": ""
}
```

**Input Request (Tiếp tục hội thoại — Resume):**

```json
{
  "event_type": "writer_chatbot",
  "model_id": "localhost",
  "user_id": "user_01",
  "conv_id": "conv_writer_789",
  "language": "Vietnamese",
  "question": "Tôi đồng ý với mô hình 5 biến độc lập, tiếp tục đi",
  "resume": true,
  "short_answer": false,
  "use_web_search_chatbot": false,
  "template_key": ""
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_type` | `str` | ✅ | Luôn là `"writer_chatbot"` |
| `model_id` | `str` | ✅ | Tên model LLM |
| `user_id` | `str` | ✅ | ID người dùng |
| `conv_id` | `str` | ✅ | ID cuộc hội thoại (dùng làm thread_id cho LangGraph) |
| `language` | `str` | ✅ | Ngôn ngữ |
| `question` | `str` | ✅ | Câu hỏi / phản hồi của người dùng |
| `resume` | `bool` | ❌ | `true` khi tiếp tục hội thoại đã bị interrupt (mặc định `false`) |
| `short_answer` | `bool` | ❌ | Trả lời ngắn gọn (mặc định `false`) |
| `use_web_search_chatbot` | `bool` | ❌ | Bật web search (mặc định `false`) |
| `template_key` | `str` | ❌ | Key template viết (mặc định `""`) |

**Output Response (Khi AI đặt câu hỏi — chưa kết thúc):**

```json
{
  "event_type": "writer_chatbot",
  "model_id": "localhost",
  "conv_id": "conv_writer_789",
  "user_id": "user_01",
  "ai_message": "Tôi đã phân tích đề tài của bạn. Trước tiên, hãy xác định các biến nghiên cứu chính...",
  "done_chat": false,
  "input_tokens": 800,
  "output_tokens": 1200,
  "template_key": "",
  "web_search_call": 0,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small"
}
```

**Output Response (Khi hoàn tất kế hoạch viết):**

```json
{
  "event_type": "writer_chatbot",
  "model_id": "localhost",
  "conv_id": "conv_writer_789",
  "user_id": "user_01",
  "done_chat": true,
  "chat_history": "...",
  "summary": {
    "title": "...",
    "variables": [...],
    "hypothesis": [...],
    "methodology": "..."
  },
  "outline": "...",
  "input_tokens": 5000,
  "output_tokens": 8000,
  "template_key": "",
  "web_search_call": 0,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small"
}
```

| Field | Type | Mô tả |
|---|---|---|
| `ai_message` | `str` | Câu hỏi/phản hồi của AI (chỉ khi `done_chat = false`) |
| `done_chat` | `bool` | `true` khi kế hoạch hoàn tất, `false` khi còn cần tương tác |
| `chat_history` | `str` | Toàn bộ lịch sử hội thoại (chỉ khi `done_chat = true`) |
| `summary` | `dict` | Tóm tắt kế hoạch viết (biến, giả thuyết, phương pháp) |
| `outline` | `str` | Dàn ý được sinh ra (chỉ khi hoàn tất) |
| `template_key` | `str` | Key template đã dùng |

---

### 4. Writer Agent (`writer`)

Agent viết báo cáo tự động bằng LangGraph workflow. Nhận toàn bộ thông tin từ writer_chatbot (summary, outline, tài liệu tham khảo) rồi viết từng chương một cách streaming.

**Input Request:**

```json
{
  "event_type": "writer",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_project_01",
  "conv_id": "conv_writer_789",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "chat_history": "...",
  "summary": {
    "title": "...",
    "variables": [...],
    "hypothesis": [...]
  },
  "outline": "...",
  "field": "Kinh tế",
  "domain": "Thương mại điện tử",
  "notes": ["Giảng viên yêu cầu tập trung vào Gen Z"],
  "use_web_search": false,
  "resume_error": false,
  "max_research_iterations": 3,
  "max_researcher_iterations": 6,
  "max_concurrent_research_units": 3
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_type` | `str` | ✅ | Luôn là `"writer"` |
| `model_id` | `str` | ✅ | Tên model LLM |
| `user_id` | `str` | ✅ | ID người dùng |
| `document_id` | `str` | ✅ | ID dự án nghiên cứu |
| `conv_id` | `str` | ✅ | ID cuộc hội thoại writer |
| `session_id` | `str` | ✅ | ID phiên làm việc |
| `language` | `str` | ✅ | Ngôn ngữ |
| `chat_history` | `str` | ✅ | Lịch sử hội thoại writer_chatbot |
| `summary` | `dict` | ✅ | Tóm tắt kế hoạch (từ writer_chatbot) |
| `outline` | `str` | ✅ | Dàn ý báo cáo |
| `field` | `str` | ❌ | Ngành lớn |
| `domain` | `str` | ❌ | Lĩnh vực |
| `notes` | `list[str]` | ❌ | Ghi chú bổ sung |
| `use_web_search` | `bool` | ❌ | Bật web search khi viết (mặc định `false`) |
| `resume_error` | `bool` | ❌ | Tiếp tục từ lỗi (mặc định `false`) |
| `max_research_iterations` | `int` | ❌ | Số vòng nghiên cứu tối đa (mặc định `3`) |
| `max_researcher_iterations` | `int` | ❌ | Số vòng researcher tối đa (mặc định `6`) |
| `max_concurrent_research_units` | `int` | ❌ | Số đơn vị nghiên cứu song song (mặc định `3`) |

**Output Response (Streaming — mỗi section viết xong):**

```json
{
  "event_type": "writer",
  "model_id": "localhost",
  "conv_id": "conv_writer_789",
  "document_id": "doc_project_01",
  "user_id": "user_01",
  "session_id": "sess_01",
  "section": {
    "name": "1.1. Tính cấp thiết của đề tài",
    "content": "Nội dung viết...",
    "refs": [...]
  },
  "change_section": true,
  "input_tokens": 10000,
  "output_tokens": 15000,
  "embed_tokens": 500,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

**Output Response (Khi hoàn tất toàn bộ báo cáo):**

```json
{
  "event_type": "writer",
  "model_id": "localhost",
  "conv_id": "conv_writer_789",
  "document_id": "doc_project_01",
  "user_id": "user_01",
  "session_id": "sess_01",
  "has_finished": true,
  "input_tokens": 50000,
  "output_tokens": 80000,
  "embed_tokens": 2000,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

| Field | Type | Mô tả |
|---|---|---|
| `section` | `dict` | Nội dung section vừa viết xong (`name`, `content`, `refs`) |
| `change_section` | `bool` | `true` khi section đã hoàn thiện và chuyển sang section mới |
| `has_finished` | `bool` | `true` khi toàn bộ báo cáo đã viết xong |

---

## Error Codes

| Error Code | Tên | Mô tả |
|---|---|---|
| `650` | `OUTER_CHATBOT` | Lỗi xử lý outer chatbot |
| `651` | `INNER_CHATBOT` | Lỗi xử lý inner chatbot |
| `652` | `WRITER_CHATBOT` | Lỗi xử lý writer chatbot |
| `653` | `WRITER` | Lỗi xử lý writer agent |
| `401` | `LLM_AUTHENTICATION_ERROR` | Lỗi xác thực API key LLM |
| `429` | `LLM_QUOTA_LIMIT` | Vượt quota LLM |
| `603` | `INVALID_PARAMS` | Thiếu trường hoặc sai kiểu dữ liệu |
| `604` | `TIME_OUT_REQUEST` | Request timeout |
| `605` | `NOT_IMPLEMENT_MODEL` | Model chưa được hỗ trợ |

## Timeout Configuration

| Event | Timeout (giây) |
|---|---|
| `outer_chatbot` | 120 |
| `inner_chatbot` | 120 |
| `writer_chatbot` | 120 |
| `writer` | 1500 |
