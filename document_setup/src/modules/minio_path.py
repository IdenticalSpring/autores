"""Chuẩn hoá đường dẫn MinIO dạng ``bucket/key``.

Các event đọc file từ MinIO (``user_guide_documents``, ``admin_documents``) nhận
đường dẫn dạng ``bucket/key``, KHÔNG phải URL đầy đủ. Module này tách bucket/key
và bỏ dấu ``/`` thừa ở đầu key để ``fget_object`` khớp đúng object trên MinIO.
"""

from exception_type import AIERROR


def parse_minio_path(path: str) -> tuple[str, str]:
    """Tách chuỗi ``bucket/key`` thành cặp ``(bucket, key)``.

    Args:
        path: Đường dẫn dạng ``bucket/key``,
            ví dụ ``"autoresearching/user-guide/guide.pdf"``.

    Returns:
        Tuple ``(bucket, key)`` trong đó ``key`` đã bỏ mọi dấu ``/`` ở đầu,
        ví dụ ``("autoresearching", "user-guide/guide.pdf")``.

    Raises:
        AIERROR: mã ``603`` (INVALID_PARAMS) khi chuỗi rỗng, thiếu dấu ``/``
            phân tách hoặc thiếu bucket/key.

    Notes:
        Không dùng ``str.replace`` để bỏ bucket: nếu tên bucket xuất hiện lại
        trong key (``autoresearching/archive/autoresearching/x.pdf``) thì
        ``replace`` sẽ cắt sai và tạo key rác.
    """
    raw = (path or "").strip()
    if "://" in raw:
        raise AIERROR(
            603,
            f"Invalid MinIO path '{path}', expected 'bucket/key' (not a full URL)",
        )

    bucket, separator, key = raw.partition("/")
    bucket = bucket.strip()
    key = key.lstrip("/").strip()

    if not bucket or not separator or not key:
        raise AIERROR(603, f"Invalid MinIO path '{path}', expected 'bucket/key'")

    return bucket, key
