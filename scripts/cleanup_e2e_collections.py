"""Dọn collection rác E2E trên Qdrant.

Mỗi lượt E2E live-tier sinh collection tạm:
  - ``0e2ee<ts>daeeef`` (outline id có timestamp)
  - ``e2e-live-<ts>-<case>`` (một số case)
Và KHÔNG có bước xoá ⇒ rác tích tụ dần, Qdrant mở thêm file (RocksDB segments)
và từng gây EMFILE ("Too many open files") — xem
``docs/AI_QDRANT_EMFILE_FIX_TODO_2026-11-01.md``.

Quy tắc an toàn:
  - CHỈ xoá collection khớp prefix trong ``DELETE_PREFIXES``.
  - KHÔNG BAO GIỜ xoá collection nằm trong ``PROTECTED`` (dữ liệu fixture
    sống mà các case E2E smoke/fixture fallback đang dùng).
  - Mặc định DRY-RUN: chỉ liệt kê kế hoạch, không xoá gì. Cờ ``--yes`` mới
    thực thi xoá.

Cách chạy (container AI không bind-mount repo host, phải copy script vào):

    cd ~/Projects/03_Work/Company/autoresearching
    docker cp scripts/cleanup_e2e_collections.py autoresearching-ai-dev:/app/scripts/
    docker exec autoresearching-ai-dev python scripts/cleanup_e2e_collections.py          # dry-run
    docker exec autoresearching-ai-dev python scripts/cleanup_e2e_collections.py --json   # cho CI
    docker exec autoresearching-ai-dev python scripts/cleanup_e2e_collections.py --yes    # xoá thật

Code thuần (is_protected / should_delete / plan) không I/O ⇒ đơn vị test được
mà không cần kết nối Qdrant.
"""

from __future__ import annotations

import argparse
import json
import sys

# Tiền tố collection rác do E2E sinh ra mỗi lượt chạy.
DELETE_PREFIXES = ("0e2ee", "e2e-live-")

# Collection cấm xoá tuyệt đối (fixture sống của smoke / fallback fixture BE).
PROTECTED = {
    "admin_user_guide",
    "650000000000000000000001",
    "650000000000000000000002",
    "e2e-live-user-documents-e70459",
    "smoke-outline-refs-1",
}


# ---------------------------------------------------------------------------
# Hàm thuần (test được, không I/O)
# ---------------------------------------------------------------------------

def is_protected(name: str, extra_keep: set[str]) -> bool:
    """True nếu tên nằm trong protect-list cứng hoặc danh sách giữ thêm."""
    return name in PROTECTED or name in extra_keep


def should_delete(name: str, extra_keep: set[str]) -> bool:
    """True nếu khớp prefix rác VÀ không nằm trong protect-list."""
    prefix_hit = any(name.startswith(p) for p in DELETE_PREFIXES)
    return prefix_hit and not is_protected(name, extra_keep)


def plan(
    names: list[str], extra_keep: set[str]
) -> tuple[list[str], list[str]]:
    """Tách danh sách collection thành (targets sẽ xoá, kept giữ lại)."""
    targets = [n for n in names if should_delete(n, extra_keep)]
    kept = [n for n in names if not should_delete(n, extra_keep)]
    return targets, kept


# ---------------------------------------------------------------------------
# Hàm I/O (Qdrant)
# ---------------------------------------------------------------------------

def _make_client(url: str):
    """Lazy import: chỉ cần qdrant_client khi thực sự chạy I/O (tránh test)."""
    from qdrant_client import QdrantClient

    return QdrantClient(url=url, check_compatibility=False)


def list_collections(
    url: str, client=None
) -> list[tuple[str, int]]:
    """Trả về [(name, points_count)] của toàn bộ collection hiện có.

    ``get_collections()`` chỉ trả tên (CollectionDescription, không có
    points_count) ⇒ phải gọi ``get_collection(name)`` thêm để lấy số point.
    """
    c = client or _make_client(url)
    infos = c.get_collections()
    colls = getattr(infos, "collections", infos)
    out: list[tuple[str, int]] = []
    for i in colls:
        try:
            out.append((i.name, c.get_collection(i.name).points_count))
        except Exception:  # noqa: BLE001 — collection đang xoá: bỏ qua số point
            out.append((i.name, -1))
    return out


def delete_collections(
    url: str, targets: list[str], dry_run: bool, client=None
) -> list[tuple[str, bool]]:
    """Xoá từng collection trong ``targets``; trả về [(name, ok)].

    Khi ``dry_run=True``: KHÔNG gọi delete, chỉ ghi [(name, True)] để kế
    hoạch hiển thị — test kiểm tra được số lời gọi delete = 0.
    """
    results: list[tuple[str, bool]] = []
    if dry_run:
        results.extend((t, True) for t in targets)
        return results

    c = client or _make_client(url)
    for name in targets:
        try:
            c.delete_collection(name)
            results.append((name, True))
        except Exception as exc:  # noqa: BLE001 — báo lỗi, tiếp tục xoá phần còn lại
            print(f"❌ xoá {name} thất bại: {exc}", file=sys.stderr)
            results.append((name, False))
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def default_url() -> str:
    """Ưu tiên settings.QDRANT_URL (nếu có), fallback localhost:6333."""
    try:
        from document_setup.src.configs.app import settings

        url = (settings.QDRANT_URL or "").strip()
        if url:
            return url
    except Exception:  # noqa: BLE001 — settings có thể chưa có biến này
        pass
    return "http://localhost:6333"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dọn collection rác E2E trên Qdrant (mặc định dry-run)."
    )
    parser.add_argument(
        "--url", default=None,
        help=f"URL Qdrant (mặc định: {default_url()})",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Thực thi xoá. Không có cờ này = chỉ dry-run.",
    )
    parser.add_argument(
        "--keep", action="append", default=[], metavar="NAME",
        help="Tên collection giữ thêm (nhiều cờ, nhiều tên).",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="In kết quả dạng JSON (cho CI).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    url = args.url or default_url()
    extra_keep = set(args.keep)

    # Validate --keep: cấm keep trùng protect-list (dư thừa, báo rõ).
    dup = extra_keep & PROTECTED
    if dup:
        print(
            f"⚠️ --keep trùng protect-list (dư thừa): {', '.join(sorted(dup))}",
            file=sys.stderr,
        )
    if not url.startswith(("http://", "https://")):
        print(f"❌ --url không hợp lệ (phải bắt đầu http:// hoặc https://): {url!r}",
              file=sys.stderr)
        return 2

    try:
        collections = list_collections(url)
    except Exception as exc:  # noqa: BLE001
        print(f"❌ Không kết nối Qdrant ({url}): {exc}", file=sys.stderr)
        return 1

    targets, _kept = plan([name for name, _ in collections], extra_keep)
    points = {name: count for name, count in collections}
    results = delete_collections(url, targets, dry_run=not args.yes)
    deleted_ok = sum(1 for _, ok in results if ok)
    deleted_fail = len(results) - deleted_ok

    if args.json:
        payload = {
            "url": url,
            "dry_run": not args.yes,
            "delete_prefixes": list(DELETE_PREFIXES),
            "protected": sorted(PROTECTED),
            "extra_keep": sorted(extra_keep),
            "plan": {
                "delete": [
                    {"name": n, "points_count": points.get(n, -1), "ok": ok}
                    for n, ok in results
                ],
                "kept": [
                    {"name": n, "points_count": points.get(n, -1),
                     "reason": "protected" if is_protected(n, extra_keep)
                     else "no-prefix"}
                    for n in _kept
                ],
            },
            "summary": {"deleted": deleted_ok, "failed": deleted_fail},
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0 if deleted_fail == 0 else 1

    # Bảng 3 cột: NAME | POINTS | ACTION
    print(f"Qdrant: {url}  |  dry-run: {not args.yes}\n")
    header = f"{'NAME':<45} {'POINTS':>8}  ACTION"
    print(header)
    print("-" * len(header))
    for name, ok in results:
        action = "delete" if args.yes else "delete (will)"
        print(f"{name:<45} {str(points.get(name, -1)):>8}  {action} [ok={ok}]")
    for name in _kept:
        action = "protected" if is_protected(name, extra_keep) else "kept"
        print(f"{name:<45} {str(points.get(name, -1)):>8}  {action}")

    total_kept = len(_kept)
    if args.yes:
        print(f"\nĐã xoá {deleted_ok} collection ({deleted_fail} lỗi).")
    else:
        print(f"\nDRY-RUN: {len(results)} sẽ xoá / {total_kept} giữ.")
    return 0 if deleted_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
