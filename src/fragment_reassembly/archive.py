"""离线回归样本档案：更正入库、整套发布、下游取数与审计。

本模块在既有 ``process`` 重组判定之上，为离线分析组提供一个可在本地
目录关闭、重开的样本档案。数据完全来自调用方传入的合成记录，载荷是
不透明字节；本模块不接触网络、不抓包、不解析任何协议。

持久化边界（详见 docs/archive.md）：

* ``HEAD`` 保存当前修订号，是唯一的事实来源，提交时原子替换；
* ``revisions/NNNNNNNN.json`` 是每个修订的更正日志，只存操作与引用；
* ``publications/<sha256>.json`` 是发布记录，冻结成员载荷与分片的引用；
* ``objects/xx/<sha256>.bin`` 按内容寻址保存载荷正文，跨修订、跨发布共享。

更正提交先写载荷对象与修订日志，最后原子替换 ``HEAD``。``HEAD`` 替换
之前的崩溃只留下无害的孤儿文件，旧修订完整可用；``HEAD`` 替换成功后
新修订、数据与发布资格作为一个整体可见。
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

from .service import process

__all__ = [
    "FragmentArchive",
    "ArchiveError",
    "ArchiveClosedError",
    "RevisionMismatchError",
    "BatchValidationError",
    "PublicationError",
    "PublicationConflictError",
    "UnknownPublicationError",
    "WithdrawnPublicationError",
    "MAX_OPERATIONS_PER_BATCH",
    "MAX_CURRENT_ROWS",
    "MAX_DATAGRAM_PAYLOAD_BYTES",
    "MAX_ROW_SPAN",
    "MAX_PUBLICATION_MEMBERS",
]

MAX_OPERATIONS_PER_BATCH = 64
MAX_CURRENT_ROWS = 10_000
MAX_DATAGRAM_PAYLOAD_BYTES = 256 * 1024
MAX_ROW_SPAN = 65_535
MAX_PAYLOAD_BYTES_PER_ROW = 65_535
MAX_PUBLICATION_MEMBERS = 64
MAX_PUBLICATION_ID_LENGTH = 256

_OP_PUT = "put"
_OP_DELETE = "delete"
_OPERATION_FIELDS = frozenset({"op", "row", "id"})
_ROW_FIELDS = ("id", "datagram_id", "offset", "more", "payload_hex")


class ArchiveError(Exception):
    """样本档案所有错误的基类。"""


class ArchiveClosedError(ArchiveError):
    """档案已关闭，不能再执行任何操作。"""


class RevisionMismatchError(ArchiveError):
    """批次声明的依据修订号与档案当前修订号不一致。

    属性 ``provided`` 是调用方声明的修订号，``current`` 是档案当前修订号。
    """

    def __init__(self, provided, current):
        self.provided = provided
        self.current = current
        super().__init__(
            f"correction batch declares base revision {provided!r} "
            f"but the archive is at revision {current}"
        )


class BatchValidationError(ArchiveError):
    """更正批次中至少一项无效，整批被拒绝。

    不产生新修订，不改变任何发布资格。属性 ``issues`` 列出全部问题。
    """

    def __init__(self, issues):
        self.issues = [str(issue) for issue in issues]
        super().__init__("invalid correction batch: " + "; ".join(self.issues))


class PublicationError(ArchiveError):
    """发布失败。发布 ID 未被占用，可以修正后重试。"""


class PublicationConflictError(PublicationError):
    """发布 ID 已被成员集合不同的发布占用，不得覆盖或重新发布。"""


class UnknownPublicationError(ArchiveError):
    """不存在该发布 ID。"""


class WithdrawnPublicationError(ArchiveError):
    """该发布已随某次更正撤回，分析取数必须失败。

    属性 ``withdrawn_at_revision`` 是撤回首次生效的修订号。
    """

    def __init__(self, publication_id, withdrawn_at_revision):
        self.publication_id = publication_id
        self.withdrawn_at_revision = withdrawn_at_revision
        super().__init__(
            f"publication {publication_id!r} was withdrawn at revision "
            f"{withdrawn_at_revision} and must not be used for analysis"
        )


@dataclass(frozen=True)
class _Row:
    """当前分片表中的一行。载荷正文不内联，只存内容寻址引用。"""

    fragment_id: str
    datagram_id: str
    offset: int
    more: bool
    payload_sha256: str
    payload_length: int

    def as_journal(self):
        return {
            "id": self.fragment_id,
            "datagram_id": self.datagram_id,
            "offset": self.offset,
            "more": self.more,
            "payload_sha256": self.payload_sha256,
            "payload_length": self.payload_length,
        }

    @classmethod
    def from_journal(cls, data):
        return cls(
            fragment_id=data["id"],
            datagram_id=data["datagram_id"],
            offset=data["offset"],
            more=data["more"],
            payload_sha256=data["payload_sha256"],
            payload_length=data["payload_length"],
        )


class FragmentArchive:
    """可关闭、重开的本地样本档案。

    ``FragmentArchive(path)`` 在 ``path`` 目录上打开档案；目录不存在或
    为空时从修订号 0 和空数据开始，否则按磁盘状态重放。``close()`` 后
    实例不可再用，重新构造即重开。支持上下文管理器。

    ``crash_hook`` 仅用于测试：若提供，会在提交边界点
    ``"journal_saved"``（修订日志已落盘、HEAD 尚未替换）和
    ``"head_saved"``（HEAD 已替换、结果尚未返回调用方）被回调。钩子
    抛出的异常原样传播，用于模拟断电；此后实例状态不再可信，应丢弃
    并重新打开档案。

    不做多进程并发写保护：同一档案同一时刻只允许一个写入者。
    """

    def __init__(self, path, *, crash_hook=None):
        self._root = Path(path)
        self._crash_hook = crash_hook
        self._closed = False
        self._objects_dir = self._root / "objects"
        self._revisions_dir = self._root / "revisions"
        self._publications_dir = self._root / "publications"
        self._head_path = self._root / "HEAD"
        for directory in (
            self._root,
            self._objects_dir,
            self._revisions_dir,
            self._publications_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self._revision = 0
        self._table = {}
        self._withdrawn = {}
        self._publications = {}
        self._load()

    # ------------------------------------------------------------------
    # 打开 / 关闭 / 重放

    def _load(self):
        head = self._read_head()
        table = {}
        withdrawn = {}
        for revision in range(1, head + 1):
            journal = self._read_json(self._revisions_dir / f"{revision:08d}.json")
            for operation in journal["ops"]:
                if operation["op"] == _OP_PUT:
                    row = _Row.from_journal(operation["row"])
                    table[row.fragment_id] = row
                else:
                    table.pop(operation["id"], None)
            for publication_id in journal["withdrawn"]:
                withdrawn.setdefault(publication_id, revision)
        publications = {}
        for path in sorted(self._publications_dir.glob("*.json")):
            record = self._read_json(path)
            publications[record["publication_id"]] = record
        self._revision = head
        self._table = table
        self._withdrawn = withdrawn
        self._publications = publications

    def _read_head(self):
        try:
            raw = self._head_path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            return 0
        return int(raw) if raw else 0

    def close(self):
        """关闭档案。所有写入在发生时已落盘，关闭只是停用本实例。"""
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
        return False

    def _ensure_open(self):
        if self._closed:
            raise ArchiveClosedError("archive is closed")

    @property
    def current_revision(self):
        """档案当前数据修订号。新档案为 0。"""
        self._ensure_open()
        return self._revision

    # ------------------------------------------------------------------
    # 更正入库

    def apply_correction(self, base_revision, operations):
        """应用一个更正批次，返回生效后的修订号。

        ``operations`` 是 1..64 项操作的列表，每项为
        ``{"op": "put", "row": {...}}`` 或 ``{"op": "delete", "id": ...}``。
        批次依据 ``base_revision`` 声明；任何一项无效则整批不生效。
        所有校验与发布资格判定只看整批应用后的完成态。合法但没有实际
        变更的批次返回原修订号，否则整批只产生一个新修订。
        """
        self._ensure_open()
        if (
            not isinstance(base_revision, int)
            or isinstance(base_revision, bool)
            or base_revision != self._revision
        ):
            raise RevisionMismatchError(base_revision, self._revision)
        if not isinstance(operations, (list, tuple)):
            raise BatchValidationError(["operations must be a list"])
        if not 1 <= len(operations) <= MAX_OPERATIONS_PER_BATCH:
            raise BatchValidationError(
                [f"batch must contain 1..{MAX_OPERATIONS_PER_BATCH} operations"]
            )

        issues = []
        parsed = []
        seen_ids = set()
        for index, operation in enumerate(operations):
            entry = self._validate_operation(index, operation, seen_ids, issues)
            if entry is not None:
                parsed.append(entry)
        if issues:
            raise BatchValidationError(issues)

        new_table = dict(self._table)
        for op_name, row, _payload in parsed:
            if op_name == _OP_PUT:
                new_table[row.fragment_id] = row
            else:
                del new_table[row]
        self._check_table_limits(new_table)

        if new_table == self._table:
            return self._revision

        # 新载荷先进入内容寻址对象库，后续的资格判定才能读到字节。
        for op_name, row, payload in parsed:
            if op_name == _OP_PUT:
                self._store_payload(payload)

        withdrawn_now = sorted(
            publication_id
            for publication_id, record in self._publications.items()
            if publication_id not in self._withdrawn
            and not self._publication_eligible(record, new_table)
        )

        new_revision = self._revision + 1
        journal = {
            "revision": new_revision,
            "base_revision": self._revision,
            "ops": [
                {"op": _OP_PUT, "row": row.as_journal()}
                if op_name == _OP_PUT
                else {"op": _OP_DELETE, "id": row}
                for op_name, row, _payload in parsed
            ],
            "withdrawn": withdrawn_now,
        }
        self._atomic_write(
            self._revisions_dir / f"{new_revision:08d}.json",
            json.dumps(journal, indent=2, sort_keys=True).encode("utf-8"),
        )
        self._run_crash_hook("journal_saved")
        self._atomic_write(self._head_path, f"{new_revision}\n".encode("ascii"))
        self._run_crash_hook("head_saved")

        self._table = new_table
        self._revision = new_revision
        for publication_id in withdrawn_now:
            self._withdrawn[publication_id] = new_revision
        return new_revision

    def _validate_operation(self, index, operation, seen_ids, issues):
        if not isinstance(operation, dict):
            issues.append(f"operation {index}: must be an object")
            return None
        unknown = sorted(set(operation) - _OPERATION_FIELDS)
        if unknown:
            issues.append(f"operation {index}: unknown fields {unknown}")
            return None
        op_name = operation.get("op")
        if op_name == _OP_PUT:
            if "row" not in operation:
                issues.append(f"operation {index}: put requires a row")
                return None
            validated = self._validate_row(index, operation["row"], issues)
            if validated is None:
                return None
            row, payload = validated
            if row.fragment_id in seen_ids:
                issues.append(
                    f"operation {index}: fragment id {row.fragment_id!r} "
                    "is operated on more than once in this batch"
                )
                return None
            seen_ids.add(row.fragment_id)
            return (_OP_PUT, row, payload)
        if op_name == _OP_DELETE:
            fragment_id = operation.get("id")
            if not isinstance(fragment_id, str) or not fragment_id:
                issues.append(
                    f"operation {index}: delete requires a non-empty string id"
                )
                return None
            if fragment_id in seen_ids:
                issues.append(
                    f"operation {index}: fragment id {fragment_id!r} "
                    "is operated on more than once in this batch"
                )
                return None
            seen_ids.add(fragment_id)
            if fragment_id not in self._table:
                issues.append(
                    f"operation {index}: cannot delete fragment id "
                    f"{fragment_id!r}: not present in the current revision"
                )
                return None
            return (_OP_DELETE, fragment_id, None)
        issues.append(f"operation {index}: op must be 'put' or 'delete'")
        return None

    def _validate_row(self, index, row, issues):
        """校验整行，返回 ``(_Row, payload_bytes)``；无效时记录问题并返回 None。"""
        if not isinstance(row, dict):
            issues.append(f"operation {index}: row must be an object")
            return None
        unknown = sorted(set(row) - set(_ROW_FIELDS))
        if unknown:
            issues.append(f"operation {index}: unknown row fields {unknown}")
            return None
        missing = [field for field in _ROW_FIELDS if field not in row]
        if missing:
            issues.append(f"operation {index}: missing row fields {missing}")
            return None
        valid = True
        fragment_id = row["id"]
        if not isinstance(fragment_id, str) or not fragment_id:
            issues.append(f"operation {index}: id must be a non-empty string")
            valid = False
        datagram_id = row["datagram_id"]
        if not isinstance(datagram_id, str) or not datagram_id:
            issues.append(
                f"operation {index}: datagram_id must be a non-empty string"
            )
            valid = False
        offset = row["offset"]
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            issues.append(
                f"operation {index}: offset must be a nonnegative integer"
            )
            valid = False
        more = row["more"]
        if not isinstance(more, bool):
            issues.append(f"operation {index}: more must be boolean")
            valid = False
        payload = None
        payload_hex = row["payload_hex"]
        if not isinstance(payload_hex, str) or len(payload_hex) % 2:
            issues.append(
                f"operation {index}: payload_hex must be an even-length "
                "hexadecimal string"
            )
            valid = False
        else:
            try:
                payload = bytes.fromhex(payload_hex)
            except ValueError:
                issues.append(
                    f"operation {index}: payload_hex is not valid hexadecimal"
                )
                valid = False
        if payload is not None:
            if not payload:
                issues.append(f"operation {index}: payload must not be empty")
                valid = False
            elif len(payload) > MAX_PAYLOAD_BYTES_PER_ROW:
                issues.append(
                    f"operation {index}: payload exceeds "
                    f"{MAX_PAYLOAD_BYTES_PER_ROW} bytes"
                )
                valid = False
        if valid and offset + len(payload) > MAX_ROW_SPAN:
            issues.append(
                f"operation {index}: offset + payload length must not "
                f"exceed {MAX_ROW_SPAN}"
            )
            valid = False
        if not valid:
            return None
        record = _Row(
            fragment_id=fragment_id,
            datagram_id=datagram_id,
            offset=offset,
            more=more,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            payload_length=len(payload),
        )
        return record, payload

    def _check_table_limits(self, table):
        """整批完成态上的档案级限制。任何一项不满足则整批不生效。"""
        issues = []
        if len(table) > MAX_CURRENT_ROWS:
            issues.append(
                f"archive would hold {len(table)} current rows, over the "
                f"{MAX_CURRENT_ROWS} row limit"
            )
        per_datagram = {}
        for row in table.values():
            per_datagram[row.datagram_id] = (
                per_datagram.get(row.datagram_id, 0) + row.payload_length
            )
        for datagram_id, total in sorted(per_datagram.items()):
            if total > MAX_DATAGRAM_PAYLOAD_BYTES:
                issues.append(
                    f"datagram {datagram_id!r} would hold {total} payload "
                    f"bytes, over the {MAX_DATAGRAM_PAYLOAD_BYTES} byte limit"
                )
        if issues:
            raise BatchValidationError(issues)

    # ------------------------------------------------------------------
    # 发布

    def publish(self, publication_id, datagram_ids):
        """发布一套样本，返回回执 ``{publication_id, revision, members}``。

        所有成员必须在当前修订都能依照 ``process`` 的判定重组成完整结果，
        否则不发布、不留半套条目、不占用发布 ID。发布成功时冻结当时的
        修订号、每个成员的重组载荷和参与重组的全部分片记录。发布 ID
        已被占用时：成员集合相同（顺序无关）返回原回执，否则报
        ``PublicationConflictError``。发布不推进数据修订。
        """
        self._ensure_open()
        if not isinstance(publication_id, str) or not publication_id:
            raise PublicationError("publication_id must be a non-empty string")
        if len(publication_id) > MAX_PUBLICATION_ID_LENGTH:
            raise PublicationError(
                f"publication_id exceeds {MAX_PUBLICATION_ID_LENGTH} characters"
            )
        if not isinstance(datagram_ids, (list, tuple)) or not all(
            isinstance(member, str) and member for member in datagram_ids
        ):
            raise PublicationError(
                "datagram_ids must be a list of non-empty strings"
            )
        members = sorted(set(datagram_ids))
        if len(members) != len(datagram_ids):
            raise PublicationError("datagram_ids must be distinct")
        if not 1 <= len(members) <= MAX_PUBLICATION_MEMBERS:
            raise PublicationError(
                f"a publication must contain 1..{MAX_PUBLICATION_MEMBERS} members"
            )

        existing = self._publications.get(publication_id)
        if existing is not None:
            if set(existing["members"]) == set(members):
                return self._receipt(existing)
            raise PublicationConflictError(
                f"publication id {publication_id!r} is already used with "
                f"members {existing['members']}"
            )

        incomplete = [
            member
            for member in members
            if not self._datagram_complete(member, self._table)
        ]
        if incomplete:
            raise PublicationError(
                f"datagrams not reassemblable to a complete result at "
                f"revision {self._revision}: {incomplete}"
            )

        record = {
            "publication_id": publication_id,
            "revision": self._revision,
            "members": members,
            "payloads": {},
            "fragments": {},
        }
        for member in members:
            rows = sorted(
                (row for row in self._table.values() if row.datagram_id == member),
                key=lambda row: row.fragment_id,
            )
            result = process(
                {"fragments": [self._row_to_fragment(row) for row in rows]}
            )
            payload_hex = result["datagrams"][0]["payload_hex"]
            record["payloads"][member] = self._store_payload(
                bytes.fromhex(payload_hex)
            )
            record["fragments"][member] = [row.as_journal() for row in rows]
        self._atomic_write(
            self._publication_path(publication_id),
            json.dumps(record, indent=2, sort_keys=True).encode("utf-8"),
        )
        self._publications[publication_id] = record
        return self._receipt(record)

    @staticmethod
    def _receipt(record):
        return {
            "publication_id": record["publication_id"],
            "revision": record["revision"],
            "members": list(record["members"]),
        }

    # ------------------------------------------------------------------
    # 下游取数（分析）与审计

    def fetch(self, publication_id):
        """分析取数：返回整套成员载荷。

        已撤回的发布抛 ``WithdrawnPublicationError``，不交付任何成员载荷；
        未知发布抛 ``UnknownPublicationError``，两者可区分。
        """
        self._ensure_open()
        record = self._publications.get(publication_id)
        if record is None:
            raise UnknownPublicationError(
                f"unknown publication id {publication_id!r}"
            )
        withdrawn_at = self._withdrawn.get(publication_id)
        if withdrawn_at is not None:
            raise WithdrawnPublicationError(publication_id, withdrawn_at)
        return {
            "publication_id": publication_id,
            "revision": record["revision"],
            "members": list(record["members"]),
            "payloads": {
                member: self._load_payload(digest).hex()
                for member, digest in record["payloads"].items()
            },
        }

    def audit(self, publication_id):
        """审计读取：不受撤回限制，返回发布时冻结的完整记录。

        包含成员、载荷、原分片记录（全部五个字段，载荷按解码值的小写
        十六进制）、发布修订号与首次撤回修订号。内容只来自发布时冻结
        的记录，绝不根据当前分片重新拼凑。
        """
        self._ensure_open()
        record = self._publications.get(publication_id)
        if record is None:
            raise UnknownPublicationError(
                f"unknown publication id {publication_id!r}"
            )
        return {
            "publication_id": publication_id,
            "revision": record["revision"],
            "members": list(record["members"]),
            "payloads": {
                member: self._load_payload(digest).hex()
                for member, digest in record["payloads"].items()
            },
            "fragments": {
                member: [
                    {
                        "id": frozen["id"],
                        "datagram_id": frozen["datagram_id"],
                        "offset": frozen["offset"],
                        "more": frozen["more"],
                        "payload_hex": self._load_payload(
                            frozen["payload_sha256"]
                        ).hex(),
                    }
                    for frozen in rows
                ]
                for member, rows in record["fragments"].items()
            },
            "withdrawn_at_revision": self._withdrawn.get(publication_id),
        }

    def publication_withdrawn_at(self, publication_id):
        """返回发布首次撤回的修订号；未撤回返回 None。未知发布抛错。"""
        self._ensure_open()
        if publication_id not in self._publications:
            raise UnknownPublicationError(
                f"unknown publication id {publication_id!r}"
            )
        return self._withdrawn.get(publication_id)

    # ------------------------------------------------------------------
    # 资格判定

    def _publication_eligible(self, record, table):
        """发布在给定（完成态）分片表下是否仍有取用资格。"""
        for rows in record["fragments"].values():
            for frozen in rows:
                current = table.get(frozen["id"])
                if current is None:
                    return False
                if (
                    current.datagram_id != frozen["datagram_id"]
                    or current.offset != frozen["offset"]
                    or current.more != frozen["more"]
                    or current.payload_sha256 != frozen["payload_sha256"]
                ):
                    return False
        for member in record["members"]:
            if not self._datagram_complete(member, table):
                return False
        return True

    def _datagram_complete(self, datagram_id, table):
        rows = [row for row in table.values() if row.datagram_id == datagram_id]
        if not rows:
            return False
        result = process({"fragments": [self._row_to_fragment(row) for row in rows]})
        return result["datagrams"][0]["status"] == "complete"

    def _row_to_fragment(self, row):
        return {
            "id": row.fragment_id,
            "datagram_id": row.datagram_id,
            "offset": row.offset,
            "more": row.more,
            "payload_hex": self._load_payload(row.payload_sha256).hex(),
        }

    # ------------------------------------------------------------------
    # 持久化原语

    def _store_payload(self, payload):
        """把载荷正文写入内容寻址对象库，返回其 sha256 十六进制。

        相同字节内容全档案只保存一份，跨修订、跨发布共享。
        """
        digest = hashlib.sha256(payload).hexdigest()
        path = self._objects_dir / digest[:2] / f"{digest}.bin"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            self._atomic_write(path, payload)
        return digest

    def _load_payload(self, digest):
        path = self._objects_dir / digest[:2] / f"{digest}.bin"
        with open(path, "rb") as handle:
            return handle.read()

    def _publication_path(self, publication_id):
        digest = hashlib.sha256(publication_id.encode("utf-8")).hexdigest()
        return self._publications_dir / f"{digest}.json"

    @staticmethod
    def _read_json(path):
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _atomic_write(self, path, data):
        """写临时文件、fsync、原子替换并 fsync 目录。"""
        temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
        with open(temporary, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        self._fsync_dir(path.parent)

    @staticmethod
    def _fsync_dir(path):
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _run_crash_hook(self, point):
        if self._crash_hook is not None:
            self._crash_hook(point)
