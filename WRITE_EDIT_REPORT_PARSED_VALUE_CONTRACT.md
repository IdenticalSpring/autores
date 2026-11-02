# Contract: `edit_report` và `parsed_value` (topic `write_section_request` → `write_section_response`)

Nguồn chuẩn: docstring đầu file
[edit_report.py](write_reports/src/modules/edit_report.py) và
[parsed_value.py](data_analysis/src/modules/parsed_value.py). Bảng dưới là bản rút gọn cho BE/FE.

Trường chung request: `event_type`, `user_id`, `document_id`, `session_id`, `model_id` (bắt buộc), `language` (mặc định `Vietnamese`).
Trường chung response: echo `event_type/user_id/document_id/session_id`, `input_tokens`, `output_tokens`, `embed_tokens=0`, `web_search_call=0`, `embed_model`, `error_code` (null = OK), `error`.

## edit_report — sửa một đoạn/section theo chỉ dẫn
| Request | Kiểu | Ghi chú |
|---|---|---|
| `content` | str, bắt buộc | Nội dung markdown gốc |
| `instruction` | str, bắt buộc | Yêu cầu chỉnh sửa |
| `section_name` | str, tuỳ chọn | Ngữ cảnh, được echo lại |
| `context` | str, tuỳ chọn | Văn bản lân cận, không bị viết lại |

Response thêm: `content` (str, nội dung đã sửa), `section_name`.
Lỗi: `603` thiếu/sai tham số · `643` lỗi xử lý · `401/403/429/5xx` lỗi LLM · `604` timeout.

## parsed_value — chuẩn hoá khảo sát thành biến
| Request | Kiểu | Ghi chú |
|---|---|---|
| `survey_questions` | list[dict], bắt buộc, không rỗng | Mỗi phần tử: `variable_name`/`variable_code` (ít nhất 1), `questions` (hoặc `question`), `variable_type` (vai trò), `measurement_type`, `answer_content` |
| `variables` | list[dict], tuỳ chọn | Mặc định `[]` |

Response thêm: `parsed_variables` (list[dict]: `name, code, statement, variable_type, role, parent_code, values, scale, unit`; enum viết thường tiếng Anh như `latent/observed`, `dependent/independent`, `interval/ratio`).
Lỗi: `603` · `639` lỗi xử lý · `401/403/429/5xx` · `604` timeout.

Payload mẫu: `scripts/payloads/edit_report.json`, `scripts/payloads/parsed_value.json`.
