# 样本档案：接口、错误与持久化边界

`fragment_reassembly.FragmentArchive` 在既有 `process` 重组判定之上，为离线
分析组提供可关闭、重开的本地样本档案，覆盖更正入库、整套样本发布、下游
取数与审计。数据只来自调用方传入的合成记录，载荷是不透明字节；不接触
网络、不抓包、不解析任何协议。原 `process(data)` 的入口、输入范围和返回
契约不变。

```python
from fragment_reassembly import FragmentArchive

archive = FragmentArchive("/path/to/archive")   # 打开或创建；空目录从修订 0 开始
revision = archive.apply_correction(0, [        # 更正入库，返回生效后的修订号
    {"op": "put", "row": {"id": "a1", "datagram_id": "A", "offset": 0,
                          "more": True, "payload_hex": "6162"}},
    {"op": "delete", "id": "a0"},
])
receipt = archive.publish("release-1", ["A"])   # 发布，回执不含载荷
bundle = archive.fetch("release-1")             # 分析取数（受撤回限制）
record = archive.audit("release-1")             # 审计读取（不受撤回限制）
archive.close()                                 # 关闭；重新构造即重开
```

## 接口

### 打开与关闭

`FragmentArchive(path)` 在本地目录上打开档案；目录不存在则创建。新档案从
修订号 0 和空数据开始。所有写入在调用返回前已落盘并 fsync，`close()` 只是
停用实例；重新构造即重开，状态由磁盘重放。支持 `with` 语句。关闭后任何
操作抛 `ArchiveClosedError`。

`current_revision` 返回当前数据修订号。

### 更正入库：`apply_correction(base_revision, operations) -> int`

- `base_revision` 必须等于当前修订号，否则抛 `RevisionMismatchError`
  （属性 `provided` / `current`）。
- `operations` 为 1 到 64 项，每项是 `{"op": "put", "row": {...}}` 或
  `{"op": "delete", "id": "..."}`。整行字段为 `id`、`datagram_id`、`offset`、
  `more`、`payload_hex`，校验规则与 `process` 一致（非空字符串 ID、非负
  整数偏移、布尔 `more`、偶数长度合法十六进制、非空载荷）。
- 档案内同一分片 ID 在当前修订最多一行；`put` 替换整行，包括变更所属
  数据报。一个批次内同一 ID 只能出现一次；删除当前不存在的 ID 是错误。
- 整批完成态上的限制：当前分片最多 10000 行；单行 `offset + 载荷字节数`
  不超过 65535；同一当前数据报的全部分片字节长度之和不超过 256 KiB
  （重叠各自计入）。校验与发布资格判定只看完成态，中间步骤不触发任何
  拒绝或撤回。
- 内容比较按解码后的字节及其余字段，与十六进制拼写大小写无关。
- 任何一项无效则整批不生效（抛 `BatchValidationError`，属性 `issues`
  列出全部问题），不推进修订、不改变任何发布资格。合法但没有实际变更
  的批次返回原修订号、不落盘；否则整批只产生一个新修订并返回它。

### 发布：`publish(publication_id, datagram_ids) -> dict`

- `datagram_ids` 为 1 到 64 个互异的非空字符串。所有成员必须在同一当前
  修订都能依照 `process` 的判定重组成完整结果，否则抛 `PublicationError`，
  不发布、不留半套条目、不占用该发布 ID（修正后可用原 ID 重试）。
- 发布成功时冻结当时的修订号、每个成员的重组载荷、参与重组的全部分片
  记录（包括内容相同的重叠分片，不只记录足以覆盖载荷的子集）。发布不
  推进数据修订。
- 回执为 `{"publication_id", "revision", "members"}`，成员按规范排序，
  不含载荷。
- 发布 ID 一旦成功占用：成员集合相同（顺序无关）的重试始终返回原回执，
  哪怕该发布当前已撤回；成员集合不同则抛 `PublicationConflictError`，
  不覆盖、不重新发布。能否取回原回执不代表现在仍可取数。

### 分析取数：`fetch(publication_id) -> dict`

返回 `{"publication_id", "revision", "members", "payloads"}`，`payloads`
按数据报 ID 给出重组载荷的十六进制。已撤回的发布抛
`WithdrawnPublicationError`（属性 `withdrawn_at_revision`），不交付任何
成员载荷；未知发布抛 `UnknownPublicationError`。两者是不同异常类型，
分析调用方可区分。

### 审计读取：`audit(publication_id) -> dict`

不受撤回限制。返回发布时冻结的记录：成员、载荷、原分片记录（全部五个
字段，载荷按解码值的小写十六进制）、发布修订号 `revision`、首次撤回修订
号 `withdrawn_at_revision`（未撤回为 `None`）。内容只来自发布时冻结的
记录，绝不根据当前分片重新拼凑。未知发布抛 `UnknownPublicationError`。

`publication_withdrawn_at(publication_id)` 只返回首次撤回修订号或 `None`。

### 撤回规则

每次更正生效后，对每个未撤回的发布按整批完成态重新判定：任一成员在
当前不再完整，或发布时冻结的任意一行在当前失去原身份或原内容（比较
全部五个字段，载荷按解码值比较），整套即随该次更正撤回。额外加入一条
不改变完整性且不修改原参与行的相同覆盖不会触发撤回。撤回记录首次生效
的修订号，不自动恢复；恢复原始分片后可用一个新发布 ID 重新发布。

## 错误一览

| 异常 | 含义 |
| --- | --- |
| `ArchiveError` | 所有档案错误的基类 |
| `ArchiveClosedError` | 档案已关闭 |
| `RevisionMismatchError` | 批次依据的修订号与当前不符 |
| `BatchValidationError` | 批次中有无效项，整批不生效（`issues` 列出问题） |
| `PublicationError` | 发布失败，发布 ID 未被占用 |
| `PublicationConflictError` | 发布 ID 已被不同成员集合占用（`PublicationError` 子类） |
| `UnknownPublicationError` | 发布 ID 不存在 |
| `WithdrawnPublicationError` | 发布已撤回，分析取数被拒绝（`withdrawn_at_revision`） |

## 持久化边界

目录布局：

```
<目录>/HEAD                        当前修订号（纯文本），唯一的事实来源
<目录>/revisions/NNNNNNNN.json     每个修订的更正日志：操作与撤回名单，只存引用
<目录>/publications/<sha256>.json  发布记录：回执、成员、冻结分片与载荷的引用
<目录>/objects/xx/<sha256>.bin     载荷正文，按内容寻址，全档案共享
```

- **数据正文共享。** 载荷正文只按解码字节的 sha256 保存一份。修订日志与
  发布记录只保存索引、引用（哈希与长度）和小回执：发布不为源分片或
  整套拼接载荷复制正文，更正也不把全档案正文另存一份。相同字节内容的
  分片、重组载荷、跨发布复用都指向同一对象。
- **原子提交。** 更正先写新载荷对象与修订日志（写临时文件、fsync、原子
  替换、fsync 目录），最后原子替换 `HEAD`。`HEAD` 替换前中断：新文件是
  无害孤儿，重开后旧修订、旧数据、旧取用资格完整可用。`HEAD` 替换后
  中断：重开后新修订、新数据与本次撤回作为一个整体可见。两个边界都不
  会出现修订、当前数据或发布资格只更新一部分的状态。发布记录单文件
  原子写入，失败不占用发布 ID。
- **重放。** 打开时读 `HEAD`，按序重放修订日志重建当前分片表与撤回名单，
  发布记录原样加载；审计内容只来自冻结的发布记录。
- **假设。** 单一写入者，不做多进程并发写保护；不限制持久格式以外的
  目录内容；孤儿对象与日志文件不影响正确性，可安全清理。
