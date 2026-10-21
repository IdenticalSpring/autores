import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exception_type import AIERROR  # noqa: E402
from document_setup.src.modules.minio_path import parse_minio_path  # noqa: E402


class TestParseMinioPath(unittest.TestCase):
    """Kiểm thử chuẩn hoá đường dẫn MinIO dạng 'bucket/key'."""

    # ---------- Happy path ----------

    def test_bucket_key_chuan(self):
        """'bucket/key' thường phải trả đúng cặp không đổi."""
        self.assertEqual(
            parse_minio_path("autoresearching/user-guide/guide.pdf"),
            ("autoresearching", "user-guide/guide.pdf"),
        )

    def test_key_nhieu_cap_thu_muc(self):
        """Key nhiều cấp giữ nguyên toàn bộ phần sau bucket."""
        self.assertEqual(
            parse_minio_path("autoresearching/a/b/c/d.pdf"),
            ("autoresearching", "a/b/c/d.pdf"),
        )

    def test_bucket_users(self):
        """Bucket 'users' (file người dùng) cũng dùng chung định dạng."""
        self.assertEqual(
            parse_minio_path("users/6838/article/doc_1/refs/p.pdf"),
            ("users", "6838/article/doc_1/refs/p.pdf"),
        )

    # ---------- Edge cases ----------

    def test_strip_dau_gach_cheo_thua(self):
        """Dấu '/' thừa ở đầu key phải bị bỏ (đây chính là bug NoSuchKey)."""
        self.assertEqual(
            parse_minio_path("autoresearching//user-guide/guide.pdf"),
            ("autoresearching", "user-guide/guide.pdf"),
        )
        self.assertEqual(
            parse_minio_path("autoresearching///user-guide/guide.pdf"),
            ("autoresearching", "user-guide/guide.pdf"),
        )

    def test_khong_cat_nham_khi_bucket_lap_lai(self):
        """Bug cũ: replace() cắt mọi lần xuất hiện của tên bucket."""
        self.assertEqual(
            parse_minio_path("autoresearching/archive/autoresearching/x.pdf"),
            ("autoresearching", "archive/autoresearching/x.pdf"),
        )

    def test_strip_khoang_trang(self):
        """Khoảng trắng thừa hai đầu chuỗi không được tạo bucket/key rác."""
        self.assertEqual(
            parse_minio_path("  autoresearching / user-guide/g.pdf  "),
            ("autoresearching", "user-guide/g.pdf"),
        )

    # ---------- Negative / Error cases ----------

    def test_rong_va_khong_dung_dinh_dang(self):
        """Chuỗi rỗng hoặc thiếu bucket/key phải trả 603 INVALID_PARAMS."""
        for gia_tri in ["", "   ", None, "autoresearching", "/guide.pdf", "autoresearching/"]:
            with self.subTest(path=gia_tri):
                with self.assertRaises(AIERROR) as ctx:
                    parse_minio_path(gia_tri)
                self.assertEqual(ctx.exception.status_code, 603)

    def test_tu_choi_url_day_du(self):
        """URL đầy đủ bị từ chối thay vì lặng lẽ lấy 'http:' làm bucket."""
        with self.assertRaises(AIERROR) as ctx:
            parse_minio_path("http://minio:9000/autoresearching/user-guide/guide.pdf")
        self.assertEqual(ctx.exception.status_code, 603)
        self.assertIn("bucket/key", ctx.exception.message)


if __name__ == "__main__":
    unittest.main()
