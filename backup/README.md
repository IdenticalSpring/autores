# 🗄️ BACKUP — Tài liệu & cấu hình đã archive

Thư mục này chứa các file **không còn là chuẩn của dự án** nhưng được **giữ lại có chủ đích**
để tra cứu và rollback. **Không dùng trực tiếp để deploy.**

| Thư mục | Nội dung | Vì sao archive |
|---|---|---|
| `docker_obsolete/` | `docker-compose-app.yaml` (5 container, topic `_local`), `docker-compose-local.yaml`, `deploy.sh` bản cũ, `scripts/*.sh` bản cũ | Trùng lặp với kiến trúc chuẩn (1 container `autoresearching-ai-dev` + 4 file compose per-service) |
| `docs_obsolete/` | Báo cáo AI, handoff, ticket, runbook cũ, `DEPLOY_legacy.md` | Thông tin lỗi thời/gây nhầm lẫn |

## Rollback một file

```bash
git mv backup/docker_obsolete/docker-compose-app.yaml .
# hoặc với file không tracked:
mv backup/docs_obsolete/<file>.md .
```

## Rollback toàn bộ

```bash
git checkout backup/pre-cleanup-<date> -- .
```

Xem chi tiết từng file + sha256 tại `MANIFEST_<date>.md`.
