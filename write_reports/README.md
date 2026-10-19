# Write Reports Module

> Module viết báo cáo nghiên cứu khoa học tự động, bao gồm: viết nội dung bài báo/luận văn/chuyên đề, phân tích dữ liệu thống kê, sinh slide thuyết trình, dịch thuật báo cáo, và đề xuất phương pháp nghiên cứu.

## Folder Structure

```
.
├── README.md
├── requirements.txt
└── src/
    ├── configs/
    │   └── app.py
    ├── modules/
    │   ├── write_graph.py           # LangGraph workflow viết nội dung chính
    │   ├── write_chat_graph.py      # LangGraph workflow cho writer chatbot
    │   ├── write_prompt_bank.py     # Prompt templates (~125KB)
    │   ├── seminar_graph_1.py       # Workflow viết chuyên đề loại 1
    │   ├── seminar_graph_2.py       # Workflow viết chuyên đề loại 2
    │   ├── seminar_graph_3.py       # Workflow viết chuyên đề loại 3 (tích hợp data analysis)
    │   ├── gen_slide_md.py          # Sinh slide dạng Markdown → PPTX
    │   ├── gen_slide_prompt.py      # Prompt cho slide generation
    │   ├── references.py            # Quản lý tài liệu tham khảo
    │   ├── translate.py             # Dịch thuật nội dung báo cáo
    │   └── log_cleaner.py           # Dọn dẹp log
    ├── schemas/
    │   ├── section.py               # Pydantic schema cho section báo cáo
    │   ├── slide.py                 # Schema cho slide
    │   └── translate.py             # Schema cho translate
    └── template/                    # Template files (docx, pptx)
```

## Kafka Topics

| Hướng | Topic (Local) | Topic (Production) |
|---|---|---|
| Request | `write_section_request_local` | `write_section_request` |
| Response | `write_section_response_local` | `write_section_response` |

---

## Events

### 1. Write Content (`write_content`)

Viết nội dung bài báo/luận văn theo từng chương dựa trên đề cương và tài liệu tham khảo. Hỗ trợ streaming — mỗi section viết xong sẽ gửi response trung gian.

**Input Request:**

```json
{
  "event_type": "write_content",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "field": "Kinh tế",
  "domain": "Thương mại điện tử",
  "references_style": "APA",
  "final_proposal": {
    "title": "Tác động của Livestream bán hàng...",
    "problem_statement": "...",
    "motivation": "..."
  },
  "papers_search": [
    {
      "title": "When we are alike: homophily in livestream commerce",
      "authors": ["Yijia Cao"],
      "year": "2025",
      "key_points": [...]
    }
  ],
  "outline": {
    "title": "...",
    "outline": [
      {
        "heading": "Chương 1: Giới thiệu",
        "word_count": "1000-1200",
        "subheadings": [...]
      }
    ]
  },
  "outline_code": ["chapter_1", "chapter_2", "chapter_3"],
  "resume": false,
  "resume_error": false,
  "resume_method": false,
  "resumeCharacter": false,
  "use_web_search": false,
  "thread_id": "uuid-xxx",
  "analyze_log": "",
  "detailed_logs": [],
  "generated_files": {},
  "db": "redis",
  "retry": 2
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_type` | `str` | ✅ | `"write_content"` |
| `model_id` | `str` | ✅ | Model LLM |
| `user_id` | `str` | ✅ | ID người dùng |
| `document_id` | `str` | ✅ | ID dự án |
| `session_id` | `str` | ✅ | ID phiên |
| `language` | `str` | ✅ | Ngôn ngữ |
| `field` | `str` | ✅ | Ngành lớn |
| `domain` | `str` | ✅ | Lĩnh vực |
| `references_style` | `str` | ✅ | Kiểu trích dẫn (`"APA"`, `"IEEE"`, ...) |
| `final_proposal` | `dict` | ✅ | Đề xuất nghiên cứu |
| `papers_search` | `list[dict]` | ✅ | Danh sách tài liệu tham khảo |
| `outline` | `dict` | ✅ | Đề cương chi tiết |
| `outline_code` | `list[str]` | ✅ | Mã code cho từng chương |
| `resume` | `bool` | ❌ | Tiếp tục viết từ checkpoint (mặc định `false`) |
| `resume_error` | `bool` | ❌ | Tiếp tục từ lỗi (mặc định `false`) |
| `resume_method` | `bool` | ❌ | Tiếp tục từ phân tích phương pháp (mặc định `false`) |
| `resumeCharacter` | `bool` | ❌ | Tiếp tục từ chương cụ thể (mặc định `false`) |
| `use_web_search` | `bool` | ❌ | Bật web search (mặc định `false`) |
| `thread_id` | `str` | ❌ | Thread ID cho LangGraph (tự sinh nếu không truyền) |
| `analyze_log` | `str` | ❌ | Log phân tích dữ liệu (cho chuyên đề 3) |
| `detailed_logs` | `list` | ❌ | Log chi tiết |
| `generated_files` | `dict` | ❌ | Files đã tạo |
| `db` | `str` | ❌ | Loại DB checkpoint (`"redis"`) |
| `retry` | `int` | ❌ | Số lần retry (mặc định `2`) |

**Output Response (Streaming — mỗi section):**

```json
{
  "event_type": "write_content",
  "model_id": "localhost",
  "document_id": "doc_01",
  "user_id": "user_01",
  "session_id": "sess_01",
  "thread_id": "uuid-xxx",
  "section": {
    "name": "1.1. Tính cấp thiết",
    "content": "Nội dung...",
    "refs": [...]
  },
  "change_section": true,
  "disable_edit": false,
  "input_tokens": 5000,
  "output_tokens": 8000,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

**Output Response (Khi cần phân tích dữ liệu — interrupt):**

```json
{
  "event_type": "write_content",
  "need_data_analyze": true,
  "section": {"name": "...", "content": ""}
}
```

**Output Response (Hoàn tất):**

```json
{
  "event_type": "write_content",
  "has_finished": true,
  "input_tokens": 50000,
  "output_tokens": 80000
}
```

---

### 2. Analyzer (`analyzer` / `analyzer_tool`)

Phân tích dữ liệu thống kê (PLS-SEM, CB-SEM, hồi quy, ...). `analyzer` trả báo cáo chi tiết, `analyzer_tool` là phiên bản nhẹ dùng như công cụ hỗ trợ.

**Input Request:**

```json
{
  "event_type": "analyzer",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "final_proposal": {"title": "..."},
  "data": {"column1": [...], "column2": [...]},
  "variables": [
    {"name": "Biến A", "code": "A", "type": "independent", "items": ["A1", "A2"]}
  ],
  "auto_plan_mode": true,
  "auto_action_mode": true,
  "manual_plan_mode": false,
  "tool_configs": [],
  "resume": false,
  "resume_error": false,
  "feedback": "",
  "thread_id": "uuid-xxx",
  "conv_id": "",
  "db": "redis"
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `data` | `dict` | ✅ | Dữ liệu khảo sát (dict of columns) |
| `variables` | `list[dict]` | ✅ | Định nghĩa biến nghiên cứu |
| `auto_plan_mode` | `bool` | ❌ | AI tự lập kế hoạch phân tích (mặc định `true`) |
| `auto_action_mode` | `bool` | ❌ | AI tự thực hiện phân tích (mặc định `true`) |
| `manual_plan_mode` | `bool` | ❌ | Chế độ thủ công (mặc định `false`) |
| `tool_configs` | `list[dict]` | ❌ | Cấu hình công cụ khi manual mode |
| `resume` | `bool` | ❌ | Tiếp tục từ checkpoint |
| `resume_error` | `bool` | ❌ | Tiếp tục từ lỗi |
| `feedback` | `str` | ❌ | Phản hồi của người dùng khi resume |
| `thread_id` | `str` | ✅ khi resume | Thread ID cho LangGraph |

**Output Response:**

```json
{
  "event_type": "analyzer",
  "document_id": "doc_01",
  "conv_id": "",
  "thread_id": "uuid-xxx",
  "has_finished": true,
  "is_tool": false,
  "analyze_log": "Kết quả phân tích chi tiết...",
  "generated_files": {"chart_1.png": "url_to_minio"},
  "detailed_logs": [...],
  "input_tokens": 10000,
  "output_tokens": 15000,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

---

### 3. Chuyên đề Loại 1 (`chuyen_de_1`)

Viết chuyên đề theo cấu trúc tổng hợp lý thuyết (không có data analysis).

**Input Request:**

```json
{
  "event_type": "chuyen_de_1",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "field": "Kinh tế",
  "domain": "Thương mại điện tử",
  "references_style": "APA",
  "final_proposal": {"title": "..."},
  "papers_search": [...],
  "outline": {"title": "...", "outline": [...]},
  "outline_code": ["chapter_1", "chapter_2"],
  "resume_error": false,
  "resumeCharacter": false,
  "use_web_search": false,
  "retry": 2
}
```

**Output Response:** Tương tự `write_content` — streaming từng section.

---

### 4. Chuyên đề Loại 2 (`chuyen_de_2`)

Viết chuyên đề theo cấu trúc phân tích thực tiễn. Tương tự `chuyen_de_1` về input/output.

---

### 5. Chuyên đề Loại 3 (`chuyen_de_3`)

Viết chuyên đề kết hợp phân tích dữ liệu thống kê. Tích hợp cả quy trình data analysis (analyzer) trước khi viết.

**Input Request:**

```json
{
  "event_type": "chuyen_de_3",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "field": "Kinh tế",
  "domain": "Thương mại điện tử",
  "references_style": "APA",
  "final_proposal": {"title": "..."},
  "papers_search": [...],
  "outline": {"title": "...", "outline": [...]},
  "outline_code": ["chapter_1", "chapter_2"],
  "data": {"column1": [...]},
  "variables": [...],
  "auto_plan_mode": true,
  "auto_action_mode": true,
  "manual_plan_mode": false,
  "tool_configs": [],
  "resume": false,
  "resume_error": false,
  "write_resume_error": false,
  "resumeCharacter": false,
  "feedback": "",
  "thread_id": "uuid-xxx",
  "conv_id": "",
  "use_web_search": false,
  "db": "redis",
  "retry": 2
}
```

| Field bổ sung | Type | Mô tả |
|---|---|---|
| `data` | `dict` | Dữ liệu khảo sát |
| `variables` | `list[dict]` | Biến nghiên cứu |
| `auto_plan_mode` | `bool` | AI tự lập plan phân tích |
| `auto_action_mode` | `bool` | AI tự thực hiện phân tích |
| `manual_plan_mode` | `bool` | Chế độ thủ công |
| `tool_configs` | `list[dict]` | Cấu hình công cụ |
| `write_resume_error` | `bool` | Tiếp tục viết từ lỗi |

---

### 6. Generate Slides (`gen_slides`)

Sinh slide thuyết trình từ nội dung báo cáo đã viết. Trả về cấu trúc slides + file PPTX.

**Input Request:**

```json
{
  "event_type": "gen_slides",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "raw_report": "<html>Nội dung báo cáo dạng HTML...</html>",
  "user_preference": 15,
  "manual_mode": false,
  "resume_error": false
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `raw_report` | `str` | ✅ | Nội dung báo cáo dạng HTML |
| `user_preference` | `int` | ✅ | Số lượng slide mong muốn |
| `manual_mode` | `bool` | ✅ | `true` nếu người dùng tự chỉnh cấu trúc slide |
| `resume_error` | `bool` | ❌ | Tiếp tục từ lỗi (mặc định `false`) |

**Output Response (Streaming — tiến trình):**

```json
{
  "event_type": "gen_slides",
  "progress": 50,
  "status": "Summarizing content (3/6)..."
}
```

**Output Response (Hoàn tất):**

```json
{
  "event_type": "gen_slides",
  "model_id": "localhost",
  "document_id": "doc_01",
  "user_id": "user_01",
  "session_id": "sess_01",
  "slides": [...],
  "pptx_url": "http://minio.../slides.pptx",
  "progress": 100,
  "status": "Completed",
  "has_finished": true,
  "input_tokens": 8000,
  "output_tokens": 12000,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

---

### 7. Translate Report (`translate_report`)

Dịch toàn bộ báo cáo hoặc đề cương sang ngôn ngữ khác.

**Input Request:**

```json
{
  "event_type": "translate_report",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "target_language": "English",
  "is_outline": false
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `target_language` | `str` | ✅ | Ngôn ngữ đích |
| `is_outline` | `bool` | ✅ | `true` để dịch đề cương, `false` để dịch báo cáo |

**Output Response:**

```json
{
  "event_type": "translate_report",
  "model_id": "localhost",
  "document_id": "doc_01",
  "user_id": "user_01",
  "session_id": "sess_01",
  "input_tokens": 5000,
  "output_tokens": 6000,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

---

### 8. Delete Report (`delete_report`)

Xóa dữ liệu báo cáo (Redis checkpoint, Qdrant collection, MongoDB records).

**Input Request:**

```json
{
  "event_type": "delete_report",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": ["doc_01", "doc_02"],
  "session_id": "sess_01",
  "thread_id": ["thread_01", "thread_02"]
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `document_id` | `list[str]` | ✅ | Danh sách document IDs cần xóa |
| `thread_id` | `list[str]` | ✅ | Danh sách thread IDs cần dọn dẹp |

---

### 9. Comment Data (`comment_data`)

Kiểm tra và nhận xét điều kiện của các biến nghiên cứu trước khi phân tích.

**Input Request:**

```json
{
  "event_type": "comment_data",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "final_proposal": {"title": "..."},
  "data": {"column1": [...]},
  "variables": [...]
}
```

**Output Response:**

```json
{
  "event_type": "comment_data",
  "comments": [
    {
      "variable_name": "Biến A",
      "condition": "passed",
      "message": "Biến A đạt yêu cầu phân phối chuẩn"
    }
  ],
  "input_tokens": 2000,
  "output_tokens": 3000,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

---

### 10. Check Logic Data (`check_logic_data`)

Kiểm tra tính logic giữa các biến nghiên cứu (trùng mã code, thiếu item, mâu thuẫn loại biến).

**Input Request:**

```json
{
  "event_type": "check_logic_data",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "variables": [
    {"name": "Biến A", "code": "A", "type": "independent", "items": ["A1", "A2"]}
  ]
}
```

**Output Response:**

```json
{
  "event_type": "check_logic_data",
  "errors_logic": ["Mã biến 'A' bị trùng với biến khác"],
  "warnings_logic": ["Item A3 không có trong dữ liệu"],
  "input_tokens": 0,
  "output_tokens": 0,
  "embed_tokens": 0,
  "embed_model": "text-embedding-3-small",
  "web_search_call": 0
}
```

---

### 11. Propose Method (`propose_method`)

AI đề xuất phương pháp nghiên cứu (biến số, giả thuyết, thang đo, bảng câu hỏi khảo sát). Sử dụng LangGraph workflow multi-turn với cơ chế interrupt/resume.

**Input Request:**

```json
{
  "event_type": "propose_method",
  "model_id": "localhost",
  "user_id": "user_01",
  "document_id": "doc_01",
  "session_id": "sess_01",
  "language": "Vietnamese",
  "thread_id": "uuid-xxx",
  "research_type": {"type": 1, "reason": "..."},
  "final_proposal": {"title": "..."},
  "user_input": "",
  "conv_id": "",
  "resume_method": false,
  "resume_error": false,
  "inherit_mode": false,
  "auto_mode": false,
  "db": "redis"
}
```

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `thread_id` | `str` | ✅ | Thread ID cho LangGraph |
| `research_type` | `dict` | ✅ | Loại nghiên cứu (`type`: 0/1/2) |
| `user_input` | `str` | ❌ | Phản hồi của người dùng khi resume |
| `resume_method` | `bool` | ❌ | Tiếp tục từ checkpoint |
| `inherit_mode` | `bool` | ❌ | Kế thừa kết quả trước đó |
| `auto_mode` | `bool` | ❌ | AI tự động hoàn tất |

**Output Response (Chưa kết thúc):**

```json
{
  "event_type": "propose_method",
  "conv_id": "",
  "thread_id": "uuid-xxx",
  "ai_message": "Dựa trên đề tài của bạn, tôi đề xuất mô hình nghiên cứu với 5 biến...",
  "has_finished": false,
  "input_tokens": 3000,
  "output_tokens": 5000
}
```

**Output Response (Hoàn tất):**

```json
{
  "event_type": "propose_method",
  "has_finished": true,
  "final_proposal": {
    "title": "...",
    "final_model": {"nodes": [...], "edges": [...]},
    "hypothesis": [{"code": "H1", "content": "..."}],
    "variables": [{"name": "...", "code": "...", "type": "independent"}],
    "survey_questions": [...],
    "parsed_variables": {...},
    "questions": [...]
  },
  "input_tokens": 10000,
  "output_tokens": 15000
}
```

---

## Error Codes

| Error Code | Tên | Mô tả |
|---|---|---|
| `630` | `WRITE_CONTENT` | Lỗi viết nội dung |
| `631` | `ANALYZER` | Lỗi phân tích dữ liệu |
| `632` | `CHUYEN_DE_1` | Lỗi viết chuyên đề 1 |
| `633` | `CHUYEN_DE_2` | Lỗi viết chuyên đề 2 |
| `634` | `GEN_SLIDES` | Lỗi sinh slide |
| `635` | `CHUYEN_DE_3` | Lỗi viết chuyên đề 3 |
| `636` | `COMMENT_DATA` | Lỗi comment data |
| `637` | `PROPOSE_METHOD` | Lỗi đề xuất phương pháp |
| `640` | `DELETE_REPORT` | Lỗi xóa báo cáo |
| `641` | `TRANSLATE_REPORT` | Lỗi dịch báo cáo |
| `642` | `CHECK_LOGIC_DATA` | Lỗi kiểm tra logic |

## Timeout Configuration

| Event | Timeout (giây) |
|---|---|
| `write_content` | 1500 |
| `analyzer` / `analyzer_tool` | 300 |
| `chuyen_de_1` | 1500 |
| `chuyen_de_2` | 1500 |
| `chuyen_de_3` | 1500 |
| `gen_slides` | 300 |
| `comment_data` | 300 |
| `propose_method` | 300 |
| `translate_report` | N/A |
| `check_logic_data` | N/A |