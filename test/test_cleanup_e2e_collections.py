"""Unit test cho scripts/cleanup_e2e_collections.py (dọn collection rác E2E).

Chỉ test các hàm thuần + hành vi dry-run của delete_collections với fake
client — KHÔNG gọi network / KHÔNG cần Qdrant thật.
"""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import cleanup_e2e_collections as cec  # noqa: E402


class FakeQdrantClient:
    """Giả lập QdrantClient: ghi lại mọi lời gọi, không I/O thật."""

    def __init__(self, collections):
        self.collections = collections
        self.deleted = []
        self.get_collections_called = 0

    def get_collections(self):
        """Giả lập QdrantClient.get_collections(): trả wrapper CollectionsInfo
        (thuộc tính .collections chứa danh sách CollectionDescription — chỉ
        có .name, không có .points_count, đúng như qdrant_client thật)."""
        self.get_collections_called += 1
        by_name = {n: p for n, p in self.collections}
        return SimpleNamespace(
            collections=[
                SimpleNamespace(name=n) for n in by_name
            ]
        )

    def get_collection(self, name):
        """Giả lập QdrantClient.get_collection(): trả điểm count theo tên."""
        points = dict(self.collections)
        return SimpleNamespace(name=name, points_count=points.get(name, 0))

    def delete_collection(self, name):
        self.deleted.append(name)


class TestShouldDelete(unittest.TestCase):
    """Lập quy tắc prefix + protect-list của should_delete."""

    def test_prefix_0e2ee_matched(self):
        """Collection 0e2ee<ts>daeeef sinh mỗi lượt E2E phải bị xoá."""
        self.assertTrue(cec.should_delete("0e2ee1793552705708daeeef", set()))

    def test_prefix_e2e_live_matched(self):
        """Collection e2e-live-<ts>-<case> phải bị xoá."""
        self.assertTrue(
            cec.should_delete("e2e-live-1793-x-update_user_refs", set())
        )

    def test_protected_wins(self):
        """e2e-live-user-documents-e70459 khớp prefix nhưng nằm trong
        protect-list ⇒ KHÔNG được xoá."""
        self.assertFalse(
            cec.should_delete("e2e-live-user-documents-e70459", set())
        )

    def test_unknown_prefix_untouched(self):
        """Collection ngoài prefix rác (kể cả cái không ai hay biết) giữ lại."""
        for name in (
            "admin_user_guide",
            "650000000000000000000001",
            "smoke-outline-refs-1",
            "my-collection",
        ):
            self.assertFalse(cec.should_delete(name, set()), name)

    def test_extra_keep_overrides(self):
        """--keep cho phép giữ thêm cả collection khớp prefix rác."""
        extra = {"e2e-live-user-documents-2b25fc"}
        self.assertTrue(cec.is_protected("e2e-live-user-documents-2b25fc", extra))
        self.assertFalse(cec.should_delete("e2e-live-user-documents-2b25fc", extra))


class TestPlan(unittest.TestCase):
    """Lập quy tắc chia (targets, kept) của plan()."""

    def test_plan_splits_targets_and_kept(self):
        names = [
            "0e2ee1793552705708daeeef",
            "admin_user_guide",
            "e2e-live-1793-x-update_user_refs",
            "e2e-live-user-documents-e70459",
        ]
        targets, kept = cec.plan(names, set())
        self.assertEqual(targets, [
            "0e2ee1793552705708daeeef",
            "e2e-live-1793-x-update_user_refs",
        ])
        self.assertEqual(kept, [
            "admin_user_guide",
            "e2e-live-user-documents-e70459",
        ])


class TestDryRunDoesNotDelete(unittest.TestCase):
    """dry_run=True phải KHÔNG gọi delete_collection trên client thật."""

    def test_dry_run_does_not_delete(self):
        client = FakeQdrantClient(
            collections=[("0e2ee1793552705708daeeef", 5)]
        )
        results = cec.delete_collections(
            "http://localhost:6333", ["0e2ee1793552705708daeeef"],
            dry_run=True, client=client,
        )
        self.assertEqual(client.deleted, [])  # 0 lời gọi delete
        self.assertEqual(
            results, [("0e2ee1793552705708daeeef", True)]
        )

    def test_exec_mode_deletes(self):
        """dry_run=False (thực thi) gọi delete đúng từng collection."""
        client = FakeQdrantClient(
            collections=[("0e2ee1793552705708daeeef", 5)]
        )
        results = cec.delete_collections(
            "http://localhost:6333", ["0e2ee1793552705708daeeef"],
            dry_run=False, client=client,
        )
        self.assertEqual(client.deleted, ["0e2ee1793552705708daeeef"])
        self.assertEqual(results, [("0e2ee1793552705708daeeef", True)])


class TestListCollections(unittest.TestCase):
    """list_collections chuyển đối tượng Qdrant thành (name, points_count)."""

    def test_list_collections_shape(self):
        client = FakeQdrantClient([("a", 1), ("b", 2)])
        self.assertEqual(
            cec.list_collections("http://x", client=client),
            [("a", 1), ("b", 2)],
        )


if __name__ == "__main__":
    unittest.main()
