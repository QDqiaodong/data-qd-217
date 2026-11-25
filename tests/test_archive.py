"""样本档案的自动化测试。全部数据均为合成记录，不接触网络。"""

import shutil
import tempfile
import unittest
from pathlib import Path

from fragment_reassembly import (
    ArchiveClosedError,
    BatchValidationError,
    FragmentArchive,
    PublicationConflictError,
    PublicationError,
    RevisionMismatchError,
    UnknownPublicationError,
    WithdrawnPublicationError,
)


def put(fragment_id, datagram_id, offset, more, payload):
    """合成一条整行写入操作。payload 为 bytes。"""
    return {
        "op": "put",
        "row": {
            "id": fragment_id,
            "datagram_id": datagram_id,
            "offset": offset,
            "more": more,
            "payload_hex": payload.hex(),
        },
    }


def delete(fragment_id):
    return {"op": "delete", "id": fragment_id}


class ArchiveTestCase(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="fragment-archive-"))
        self.addCleanup(shutil.rmtree, self.root, True)

    def open_archive(self, **kwargs):
        archive = FragmentArchive(self.root, **kwargs)
        self.addCleanup(archive.close)
        return archive

    def object_files(self):
        return list((self.root / "objects").rglob("*.bin"))

    def seed_standard(self, archive):
        """修订 1：A 由 a1/a2（等值覆盖）+ a3（终点）组成，B 由 b1 组成。"""
        revision = archive.apply_correction(
            0,
            [
                put("a1", "A", 0, True, b"ab"),
                put("a2", "A", 0, True, b"ab"),
                put("a3", "A", 2, False, b"cd"),
                put("b1", "B", 0, False, b"\x0a\x0b"),
            ],
        )
        self.assertEqual(revision, 1)
        return revision


class CorrectionBatchTests(ArchiveTestCase):
    def test_new_archive_starts_empty_at_revision_zero(self):
        archive = self.open_archive()
        self.assertEqual(archive.current_revision, 0)

    def test_revision_mismatch_rejects_whole_batch(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        with self.assertRaises(RevisionMismatchError) as caught:
            archive.apply_correction(5, [put("x1", "X", 0, False, b"zz")])
        self.assertEqual(caught.exception.current, 1)
        self.assertEqual(archive.current_revision, 1)

    def test_batch_size_limits(self):
        archive = self.open_archive()
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(0, [])
        too_many = [put(f"f{i}", "D", 0, False, b"x") for i in range(65)]
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(0, too_many)
        self.assertEqual(archive.current_revision, 0)

    def test_same_id_only_once_per_batch(self):
        archive = self.open_archive()
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(
                0, [put("a1", "A", 0, False, b"ab"), delete("a1")]
            )
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(
                0,
                [
                    put("a1", "A", 0, False, b"ab"),
                    put("a1", "A", 0, False, b"cd"),
                ],
            )

    def test_delete_of_unknown_id_is_an_error(self):
        archive = self.open_archive()
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(0, [delete("nope")])

    def test_invalid_rows_are_rejected(self):
        archive = self.open_archive()
        bad_rows = [
            put("a1", "A", 0, False, b""),  # 空载荷
            put("a1", "A", -1, False, b"ab"),  # 负偏移
            put("a1", "A", True, False, b"ab"),  # 布尔偏移
            put("a1", "A", 0, 1, b"ab"),  # more 非布尔
            put("a1", "", 0, False, b"ab"),  # 空数据报 ID
            put("", "A", 0, False, b"ab"),  # 空分片 ID
        ]
        for operation in bad_rows:
            with self.assertRaises(BatchValidationError, msg=operation):
                archive.apply_correction(0, [operation])
        bad_hex = put("a1", "A", 0, False, b"ab")
        bad_hex["row"]["payload_hex"] = "zz"
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(0, [bad_hex])
        odd_hex = put("a1", "A", 0, False, b"ab")
        odd_hex["row"]["payload_hex"] = "abc"
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(0, [odd_hex])
        extra_field = put("a1", "A", 0, False, b"ab")
        extra_field["row"]["note"] = "nope"
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(0, [extra_field])
        self.assertEqual(archive.current_revision, 0)

    def test_offset_plus_length_must_not_exceed_65535(self):
        archive = self.open_archive()
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(0, [put("a1", "A", 65535, False, b"x")])
        revision = archive.apply_correction(
            0, [put("a1", "A", 65534, False, b"x")]
        )
        self.assertEqual(revision, 1)

    def test_datagram_payload_byte_limit(self):
        archive = self.open_archive()
        block = bytes(65535)
        ok_ops = [put(f"f{i}", "D", 0, False, block) for i in range(4)]
        self.assertEqual(archive.apply_correction(0, ok_ops), 1)
        with self.assertRaises(BatchValidationError) as caught:
            archive.apply_correction(1, [put("f4", "D", 0, False, block)])
        self.assertIn("byte limit", str(caught.exception))
        self.assertEqual(archive.current_revision, 1)

    def test_current_row_limit(self):
        archive = self.open_archive()
        next_id = 0

        def fill(count):
            nonlocal next_id
            ops = []
            for _ in range(count):
                ops.append(put(f"f{next_id}", f"dg-{next_id}", 0, False, b"\x01"))
                next_id += 1
            return ops

        revision = 0
        for _ in range(156):  # 156 * 64 = 9984 行
            revision = archive.apply_correction(revision, fill(64))
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(revision, fill(64))  # 10048 > 10000
        revision = archive.apply_correction(revision, fill(16))  # 恰好 10000
        self.assertEqual(archive.current_revision, revision)
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(revision, fill(1))
        # 删除一行后就有空位
        revision = archive.apply_correction(revision, [delete("f0")])
        archive.apply_correction(revision, fill(1))

    def test_noop_batch_returns_same_revision_and_writes_nothing(self):
        archive = self.open_archive()
        archive.apply_correction(0, [put("a1", "A", 0, False, b"\xab\xcd")])
        same = put("a1", "A", 0, False, b"\xab\xcd")
        self.assertEqual(archive.apply_correction(1, [same]), 1)
        # 十六进制拼写大小写不改变解码后的字节，也不是实际变更
        upper = put("a1", "A", 0, False, b"")
        upper["row"]["payload_hex"] = "ABCD"
        self.assertEqual(archive.apply_correction(1, [upper]), 1)
        self.assertFalse((self.root / "revisions" / "00000002.json").exists())

    def test_put_replaces_whole_row_including_datagram(self):
        archive = self.open_archive()
        archive.apply_correction(0, [put("a1", "A", 0, False, b"xy")])
        revision = archive.apply_correction(1, [put("a1", "B", 0, False, b"xy")])
        self.assertEqual(revision, 2)
        with self.assertRaises(PublicationError):
            archive.publish("PA", ["A"])  # A 已没有任何分片
        receipt = archive.publish("PB", ["B"])
        self.assertEqual(receipt["revision"], 2)
        self.assertEqual(archive.fetch("PB")["payloads"]["B"], b"xy".hex())

    def test_mixed_valid_write_and_invalid_delete_changes_nothing(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        archive.publish("P1", ["A"])
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(
                1, [put("a9", "A", 0, True, b"ab"), delete("missing-id")]
            )
        self.assertEqual(archive.current_revision, 1)  # 无修订推进
        self.assertFalse((self.root / "revisions" / "00000002.json").exists())
        # 无误撤回：发布仍然可取
        self.assertEqual(archive.fetch("P1")["payloads"]["A"], b"abcd".hex())

    def test_closed_archive_rejects_operations(self):
        archive = FragmentArchive(self.root)
        archive.close()
        with self.assertRaises(ArchiveClosedError):
            archive.apply_correction(0, [put("a1", "A", 0, False, b"ab")])
        with self.assertRaises(ArchiveClosedError):
            archive.fetch("P1")
        with self.assertRaises(ArchiveClosedError):
            archive.current_revision


class PublicationTests(ArchiveTestCase):
    def test_receipt_has_no_payload_and_members_are_sorted(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        receipt = archive.publish("P1", ["B", "A"])
        self.assertEqual(
            receipt,
            {"publication_id": "P1", "revision": 1, "members": ["A", "B"]},
        )

    def test_publish_does_not_advance_revision(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        archive.publish("P1", ["A"])
        self.assertEqual(archive.current_revision, 1)

    def test_incomplete_member_fails_and_id_stays_free(self):
        archive = self.open_archive()
        archive.apply_correction(
            0,
            [
                put("e1", "E", 0, True, b"x"),  # E 缺终点，不完整
                put("f1", "F", 0, False, b"y"),
            ],
        )
        with self.assertRaises(PublicationError):
            archive.publish("PE", ["E", "F"])
        self.assertFalse(list((self.root / "publications").glob("*.json")))
        archive.apply_correction(1, [put("e2", "E", 1, False, b"z")])
        receipt = archive.publish("PE", ["E", "F"])  # 原 ID 可重新发布
        self.assertEqual(receipt["revision"], 2)
        self.assertEqual(archive.fetch("PE")["payloads"]["E"], b"xz".hex())

    def test_conflicting_and_missing_members_fail(self):
        archive = self.open_archive()
        archive.apply_correction(
            0,
            [
                put("c1", "C", 0, False, b"\x00"),
                put("c2", "C", 0, False, b"\xff"),  # 与 c1 冲突
            ],
        )
        with self.assertRaises(PublicationError):
            archive.publish("PC", ["C"])
        with self.assertRaises(PublicationError):
            archive.publish("PM", ["missing"])

    def test_publish_input_validation(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        with self.assertRaises(PublicationError):
            archive.publish("", ["A"])
        with self.assertRaises(PublicationError):
            archive.publish("P", [])
        with self.assertRaises(PublicationError):
            archive.publish("P", ["A", "A"])
        with self.assertRaises(PublicationError):
            archive.publish("P", "A")
        with self.assertRaises(PublicationError):
            archive.publish("P", [f"dg-{i}" for i in range(65)])

    def test_same_id_different_members_is_a_conflict(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        archive.publish("P1", ["A"])
        with self.assertRaises(PublicationConflictError):
            archive.publish("P1", ["A", "B"])
        with self.assertRaises(PublicationConflictError):
            archive.publish("P1", ["B"])

    def test_unknown_and_withdrawn_are_distinguishable(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        archive.publish("P1", ["A"])
        with self.assertRaises(UnknownPublicationError):
            archive.fetch("nope")
        with self.assertRaises(UnknownPublicationError):
            archive.audit("nope")
        archive.apply_correction(1, [delete("a1")])
        with self.assertRaises(WithdrawnPublicationError) as caught:
            archive.fetch("P1")
        self.assertEqual(caught.exception.withdrawn_at_revision, 2)
        self.assertNotEqual(WithdrawnPublicationError, UnknownPublicationError)

    def test_audit_returns_frozen_snapshot_not_current_data(self):
        archive = self.open_archive()
        archive.apply_correction(0, [put("a1", "A", 0, False, b"abcd")])
        archive.publish("P1", ["A"])
        # 替换 a1 的内容：发布随该次更正撤回
        revision = archive.apply_correction(1, [put("a1", "A", 0, False, b"wxyz")])
        self.assertEqual(revision, 2)
        with self.assertRaises(WithdrawnPublicationError):
            archive.fetch("P1")
        record = archive.audit("P1")
        self.assertEqual(record["payloads"]["A"], b"abcd".hex())
        self.assertEqual(
            record["fragments"]["A"],
            [
                {
                    "id": "a1",
                    "datagram_id": "A",
                    "offset": 0,
                    "more": False,
                    "payload_hex": b"abcd".hex(),
                }
            ],
        )
        self.assertEqual(record["withdrawn_at_revision"], 2)

    def test_publication_withdrawn_at(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        archive.publish("P1", ["A"])
        self.assertIsNone(archive.publication_withdrawn_at("P1"))
        archive.apply_correction(1, [delete("a1")])
        self.assertEqual(archive.publication_withdrawn_at("P1"), 2)
        with self.assertRaises(UnknownPublicationError):
            archive.publication_withdrawn_at("nope")


class SharedDatagramWithdrawalTests(ArchiveTestCase):
    """两套发布共用数据报 A，另一套只用 B 的完整生命周期。"""

    def test_shared_datagram_scenario(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        receipt_a1 = archive.publish("P1", ["A"])
        receipt_a2 = archive.publish("P2", ["A", "B"])
        receipt_b = archive.publish("P3", ["B"])
        for receipt in (receipt_a1, receipt_a2, receipt_b):
            self.assertEqual(receipt["revision"], 1)
        self.assertEqual(archive.fetch("P1")["payloads"], {"A": b"abcd".hex()})
        self.assertEqual(
            archive.fetch("P2")["payloads"],
            {"A": b"abcd".hex(), "B": b"\x0a\x0b".hex()},
        )
        self.assertEqual(archive.fetch("P3")["payloads"], {"B": b"\x0a\x0b".hex()})

        # 额外加入一条等值覆盖且保留原行：三套都仍可取
        revision = archive.apply_correction(1, [put("a4", "A", 0, True, b"ab")])
        self.assertEqual(revision, 2)
        for publication_id in ("P1", "P2", "P3"):
            archive.fetch(publication_id)

        # 删除 A 的一条原参与行：A 仍完整且载荷不变，但两套含 A 的发布
        # 在同一修订撤回，只用 B 的发布不受影响
        revision = archive.apply_correction(2, [delete("a1")])
        self.assertEqual(revision, 3)
        for publication_id in ("P1", "P2"):
            with self.assertRaises(WithdrawnPublicationError) as caught:
                archive.fetch(publication_id)
            self.assertEqual(caught.exception.withdrawn_at_revision, 3)
        self.assertEqual(archive.fetch("P3")["payloads"], {"B": b"\x0a\x0b".hex()})
        # A 在当前修订仍完整、载荷不变：可用新发布 ID 发布当前样本
        receipt_now = archive.publish("P4", ["A"])
        self.assertEqual(receipt_now["revision"], 3)
        self.assertEqual(archive.fetch("P4")["payloads"]["A"], b"abcd".hex())

        # 恢复原参与行
        revision = archive.apply_correction(3, [put("a1", "A", 0, True, b"ab")])
        self.assertEqual(revision, 4)
        archive.close()

        # 关闭并重开：旧的两套仍不可用于分析，撤回不自动恢复
        archive = self.open_archive()
        self.assertEqual(archive.current_revision, 4)
        for publication_id in ("P1", "P2"):
            with self.assertRaises(WithdrawnPublicationError):
                archive.fetch(publication_id)
        archive.fetch("P3")
        archive.fetch("P4")

        # 审计读出的旧版保留原行，不混入发布后额外加入的等值覆盖 a4
        record = archive.audit("P1")
        self.assertEqual(record["revision"], 1)
        self.assertEqual(record["withdrawn_at_revision"], 3)
        self.assertEqual(record["payloads"]["A"], b"abcd".hex())
        self.assertEqual(
            {row["id"] for row in record["fragments"]["A"]}, {"a1", "a2", "a3"}
        )
        record = archive.audit("P2")
        self.assertEqual(record["withdrawn_at_revision"], 3)
        self.assertEqual(
            {row["id"] for row in record["fragments"]["A"]}, {"a1", "a2", "a3"}
        )
        self.assertEqual(
            {row["id"] for row in record["fragments"]["B"]}, {"b1"}
        )

        # 同发布 ID、同成员集合（顺序不同）的重试取回原回执
        self.assertEqual(archive.publish("P1", ["A"]), receipt_a1)
        self.assertEqual(archive.publish("P2", ["B", "A"]), receipt_a2)
        # 成员集合不同则报错
        with self.assertRaises(PublicationConflictError):
            archive.publish("P1", ["A", "B"])

        # 恢复原始分片后，用新发布 ID 才能发布当前样本
        receipt_new = archive.publish("P5", ["A"])
        self.assertEqual(receipt_new["revision"], 4)
        self.assertEqual(archive.fetch("P5")["payloads"]["A"], b"abcd".hex())
        # 新发布冻结的是当前参与行，包含 a4
        self.assertEqual(
            {row["id"] for row in archive.audit("P5")["fragments"]["A"]},
            {"a1", "a2", "a3", "a4"},
        )

        # 载荷正文按内容共享：全部历史只有 4 份不同的正文
        self.assertEqual(len(self.object_files()), 4)


class LargeDatagramTests(ArchiveTestCase):
    """63 KiB 等值分片与整批完成态判定。"""

    def test_intermediate_state_is_never_judged(self):
        archive = self.open_archive()
        block = b"\x5a" * (63 * 1024)  # 63 KiB
        revision = archive.apply_correction(
            0,
            [
                put("d1", "D", 0, False, block),
                put("d2", "D", 0, False, block),
                put("d3", "D", 0, False, block),
            ],
        )
        self.assertEqual(revision, 1)  # 3 * 63 KiB = 189 KiB
        archive.publish("PD", ["D"])
        # 追加第四条等值且未被固定的分片：4 * 63 KiB = 252 KiB
        revision = archive.apply_correction(1, [put("d4", "D", 0, False, block)])
        self.assertEqual(revision, 2)
        self.assertEqual(archive.fetch("PD")["payloads"]["D"], block.hex())

        # 同一批次先加第五条等值分片、再删第四条：中间态 5 * 63 KiB
        # 超过 256 KiB，完成态只有 252 KiB，批次必须成功
        revision = archive.apply_correction(
            2, [put("d5", "D", 0, False, block), delete("d4")]
        )
        self.assertEqual(revision, 3)
        # 原发布继续可取：d4 不是发布时的参与行，d5 只是额外等值覆盖
        self.assertEqual(archive.fetch("PD")["payloads"]["D"], block.hex())
        # 全部历史只有一份正文：等值分片与重组载荷共享同一对象
        self.assertEqual(len(self.object_files()), 1)

        # 混合有效写入与无效删除：无修订推进、无误撤回
        with self.assertRaises(BatchValidationError):
            archive.apply_correction(
                3, [put("d6", "D", 0, False, block), delete("missing-id")]
            )
        self.assertEqual(archive.current_revision, 3)
        self.assertFalse((self.root / "revisions" / "00000004.json").exists())
        self.assertEqual(archive.fetch("PD")["payloads"]["D"], block.hex())


class SimulatedCrash(Exception):
    pass


class CrashRecoveryTests(ArchiveTestCase):
    """对会同时撤回两套发布的更正，验证两个提交边界的中断恢复。"""

    def hook_failing_at(self, point):
        def hook(at):
            if at == point:
                raise SimulatedCrash(at)

        return hook

    def seed_and_publish(self):
        archive = FragmentArchive(self.root)
        self.seed_standard(archive)
        archive.publish("P1", ["A"])
        archive.publish("P2", ["A"])
        archive.publish("P3", ["B"])
        archive.close()

    def test_crash_before_commit_keeps_old_revision_and_eligibility(self):
        self.seed_and_publish()
        archive = FragmentArchive(
            self.root, crash_hook=self.hook_failing_at("journal_saved")
        )
        with self.assertRaises(SimulatedCrash):
            archive.apply_correction(1, [delete("a1")])

        reopened = self.open_archive()
        self.assertEqual(reopened.current_revision, 1)
        # 旧修订、原取用资格都在：三套发布照常取数
        self.assertEqual(reopened.fetch("P1")["payloads"]["A"], b"abcd".hex())
        self.assertEqual(reopened.fetch("P2")["payloads"]["A"], b"abcd".hex())
        self.assertEqual(reopened.fetch("P3")["payloads"]["B"], b"\x0a\x0b".hex())
        # 修订、当前数据、发布资格没有出现只更新一部分的状态：
        # 依据旧修订号的更正仍被接受，且孤儿日志不影响后续提交
        self.assertEqual(reopened.apply_correction(1, [delete("a1")]), 2)
        with self.assertRaises(WithdrawnPublicationError):
            reopened.fetch("P1")

    def test_crash_after_commit_exposes_new_revision_and_withdrawals(self):
        self.seed_and_publish()
        archive = FragmentArchive(
            self.root, crash_hook=self.hook_failing_at("head_saved")
        )
        with self.assertRaises(SimulatedCrash):
            archive.apply_correction(1, [delete("a1")])

        reopened = self.open_archive()
        # 新修订和两套撤回一并可见
        self.assertEqual(reopened.current_revision, 2)
        for publication_id in ("P1", "P2"):
            with self.assertRaises(WithdrawnPublicationError) as caught:
                reopened.fetch(publication_id)
            self.assertEqual(caught.exception.withdrawn_at_revision, 2)
        reopened.fetch("P3")
        # 原发布仍可审计：成员、载荷、原分片记录、发布与撤回修订都在
        record = reopened.audit("P1")
        self.assertEqual(record["revision"], 1)
        self.assertEqual(record["withdrawn_at_revision"], 2)
        self.assertEqual(record["payloads"]["A"], b"abcd".hex())
        self.assertEqual(
            {row["id"] for row in record["fragments"]["A"]}, {"a1", "a2", "a3"}
        )
        # 当前数据与新修订一致：A 仍完整，可重新发布
        receipt = reopened.publish("P4", ["A"])
        self.assertEqual(receipt["revision"], 2)
        self.assertEqual(reopened.fetch("P4")["payloads"]["A"], b"abcd".hex())

    def test_crash_boundaries_are_reproducible(self):
        # 同一更正先在“保存完成前”中断，重试又在“已保存未交付”中断，
        # 两次重开都只能看到完整的新态或完整的旧态。
        self.seed_and_publish()
        archive = FragmentArchive(
            self.root, crash_hook=self.hook_failing_at("journal_saved")
        )
        with self.assertRaises(SimulatedCrash):
            archive.apply_correction(1, [delete("a1")])
        first = self.open_archive()
        self.assertEqual(first.current_revision, 1)
        first.fetch("P1")
        first.fetch("P2")
        first.close()

        archive = FragmentArchive(
            self.root, crash_hook=self.hook_failing_at("head_saved")
        )
        with self.assertRaises(SimulatedCrash):
            archive.apply_correction(1, [delete("a1")])
        second = self.open_archive()
        self.assertEqual(second.current_revision, 2)
        with self.assertRaises(WithdrawnPublicationError):
            second.fetch("P1")
        with self.assertRaises(WithdrawnPublicationError):
            second.fetch("P2")
        second.fetch("P3")


class PersistenceTests(ArchiveTestCase):
    def test_reopen_replays_revisions_and_publications(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        archive.apply_correction(1, [put("c1", "C", 0, False, b"hello")])
        archive.publish("P1", ["A", "C"])
        archive.close()

        reopened = self.open_archive()
        self.assertEqual(reopened.current_revision, 2)
        result = reopened.fetch("P1")
        self.assertEqual(result["revision"], 2)
        self.assertEqual(
            result["payloads"], {"A": b"abcd".hex(), "C": b"hello".hex()}
        )
        self.assertIsNone(reopened.audit("P1")["withdrawn_at_revision"])

    def test_payload_bodies_are_shared_not_duplicated(self):
        archive = self.open_archive()
        self.seed_standard(archive)
        archive.publish("P1", ["A"])
        # 发布只新增一份按内容寻址的重组载荷（"abcd"），源分片正文不复制
        after_first = len(self.object_files())
        archive.publish("P2", ["A", "B"])
        archive.publish("P3", ["B"])
        # 后续发布完全共享已有正文，一个文件都不增加
        self.assertEqual(len(self.object_files()), after_first)
        # 更正也不另存全档案正文：只新增真正新内容的那一份
        archive.apply_correction(1, [put("c1", "C", 0, False, b"hello")])
        self.assertEqual(len(self.object_files()), after_first + 1)


if __name__ == "__main__":
    unittest.main()
