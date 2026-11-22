# 隔离网关维护消息接收端 —— 离线模拟器

纯 CPU、纯标准库的离线模拟器，实现隔离网关上「维护消息接收端」的
接收窗口、世代交接、批准规则、跨来源因果前置、策略屏障、审计 gap、
环路处理与持久回执。**运行时不会建立任何真实连接**：没有 socket、没有
HTTP 客户端、没有子进程，唯一输入是本地 JSON 案例文件与本地 SQLite 文件，
唯一输出是 stdout 上的一份确定性 UTF-8 JSON。

- 平台：Windows 原生 Python 3.12，只用标准库（`json`、`sqlite3`、`hashlib`、`unittest`）。
- 依赖：无第三方包；不需要网络；不需要 GPU。
- 入口：`python -m solution <命令> CASE.json`

---

## 1. 快速开始

```bash
# 1) 内存演练：不落地任何状态
python -m solution simulate samples/01_two_gateways.json

# 2) 持久接收：把回执写进 SQLite
python -m solution ingest samples/01_two_gateways.json --db demo.sqlite

# 3) 读回全部回执（含每条结果的规则版本）
python -m solution dump   samples/01_two_gateways.json --db demo.sqlite

# 4) 查看窗口、缓冲区、下一代门闸、最短环
python -m solution inspect samples/02_generation_gap.json --db demo2.sqlite

# 5) 校验数据库结构与内容校验和
python -m solution verify  samples/01_two_gateways.json --db demo.sqlite

# 6) 断言测试
python -m unittest discover -s tests -v
```

同一个 `ingest` 案例重复执行是幂等的：第二次不会再发放任何回执。

---

## 2. 命令一览

```
python -m solution simulate CASE.json
python -m solution ingest   CASE.json --db DB.sqlite
python -m solution dump     CASE.json --db DB.sqlite
python -m solution inspect  CASE.json --db DB.sqlite
python -m solution verify   CASE.json --db DB.sqlite      # 别名：verify-db
```

| 命令 | 需要 `--db` | 写数据库 | 说明 |
|------|-------------|----------|------|
| `simulate` | 否（给了会报错） | 否 | 在内存里跑完整个案例，返回所有回执 |
| `ingest` | 是 | 是，单事务 | 把一个案例的全部动作原子地应用到持久状态 |
| `dump` | 是 | 否 | 返回每条结果及其规则版本、窗口、缓冲、环路 |
| `inspect` | 是 | 否 | 返回阻塞点、最短环、被门闸挡住的世代 |
| `verify` | 是 | 否 | 校验 schema、回执连续性和内容校验和 |

`dump` / `inspect` / `verify` 的案例文件只需含 `case_id`；给它完整案例文件
也可以（会校验 `case_id` 与数据库一致）。`--db=DB.sqlite` 写法同样接受。

### 退出码

| 退出码 | 含义 |
|--------|------|
| 0 | 成功（`verify` 表示数据库健康） |
| 1 | 其他运行期错误 |
| 2 | 命令行用法错误（`E_USAGE`） |
| 3 | 输入结构 / JSON / 语义错误 |
| 4 | 数据库错误（缺失、不可读、校验失败、写失败） |

任何失败都返回 `{"ok": false, "error": {"code", "path", "detail"}}`，
`path` 是定位到字段的 JSON Pointer，例如 `batches/0/events/1/fingerprint`。

---

## 3. 输入：案例文件

顶层字段（全部字段名大小写敏感；出现未知字段直接报 `E_FIELD_UNKNOWN`）：

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `case_id` | string | 是 | — | 案例标识；一个数据库只接受同一个 `case_id` |
| `max_id_len` | int | 否 | 128 | 所有标识符的最大码点数（1..512） |
| `control` | object | 否 | `{}` | 见下表 |
| `announcements` | array | 是 | — | 可信世代公告 |
| `rules` | array | 是 | — | 批准的源→目的规则（规则版本表） |
| `batches` | array | 否 | `[]` | 定长事件批（乱序到达） |
| `gaps` | array | 否 | `[]` | 审计方签发的 gap / 取消指令 |

### 3.1 `control`

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `max_window` | int (1..256) | 8 | 接收窗口宽度：`[cursor, cursor+max_window-1]` |
| `max_batch` | int (1..4096) | 64 | 单批事件数上限 |
| `batch_size` | int 或 null | null | 非空时要求每个批恰好这么多事件 |
| `strict_fields` | bool | true | 标识符只允许 `[A-Za-z0-9._:-]` |
| `fail_after` | int (0..10000) 或 null | null | 故障注入：写满 N 条回执后中止 |
| `failpoint` | `"before_commit"` / `"after_commit"` 或 null | null | 故障注入点 |

### 3.2 `announcements[]`（可信世代公告）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `src` | string | 是 | 来源 |
| `gen` | int ≥ 1 | 是 | 来源世代；**只与同一来源的相邻世代比较** |
| `start_seq` | int ≥ 0 | 是 | 该世代的起始序号（窗口初始 cursor） |
| `prev_end_seq` | int ≥ 0 或 null | 否 | 上一世代的**结束序号**；声明后本世代可预收，但门闸关闭 |

约束：

- 同一 `(src, gen)` 只能公告一次；重复且内容不同 → `E_ANNOUNCEMENT_CONFLICT`。
- 带 `prev_end_seq` 时，上一世代 `gen-1` 必须已公告（`E_ANNOUNCEMENT_PREV_MISSING`），
  且 `prev_end_seq >= 上世代 start_seq`（`E_ANNOUNCEMENT_BOUNDS`）。
- 序号只在世代内部递增；不同世代的序号不排序、不比较大小。

### 3.3 `rules[]`（批准的源→目的规则；版本化）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `id` | string | 是 | 规则 id；同一 id 可有多个版本 |
| `effective_epoch` | int ≥ 0 | 是 | 该版本生效的逻辑时刻（策略屏障） |
| `precedence` | int ≥ 0 | 否 | 同 epoch 时的优先级，默认 0 |
| `routes` | array | 是 | `[{"dst": string, "allow": bool}]`，同一 `dst` 只能出现一次 |

规则版本表既可以在顶层 `rules` 给出，也可以挂在某个事件批的 `rules` 上
（事务内生效的紧急策略）。安装时的结构检查：

- 同 `(id, effective_epoch)` 但内容不同 → `E_RULE_VERSION_CONFLICT`（拒绝整批）；
- 不同 `id` 但同 epoch、同 precedence、又都提到了同一个 `dst` → `E_RULE_AMBIGUOUS`。

裁定规则：对某条消息，取**所有 `effective_epoch <= 当前逻辑时刻` 且覆盖其
`dst` 的版本中最新的一版**。逻辑时刻只在事务开始时推进（取本批 `epoch`），
所以一条从等待态解封的消息在下一次裁定时必定读到新版本，不会读到被撤销的旧版本。

### 3.4 `batches[]`（定长事件批）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `id` | string | 是 | 批 id；`(arrival, id)` 在文件内唯一 |
| `arrival` | int ≥ 0 | 是 | 逻辑到达时刻，决定批的处理顺序 |
| `epoch` | int ≥ 0 | 是 | 本事务的逻辑时刻（策略屏障） |
| `events` | array | 是 | 至少 1 条、至多 `max_batch` 条 |
| `rules` | array | 否 | 本批内安装的规则版本 |

时间线：所有动作按 `(arrival, 0=批/1=gap, id)` 排序；同一 arrival 先处理批、再处理 gap。

### 3.5 `batches[].events[]`（单条维护消息）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `gateway` | string | 是 | 转发该消息的网关 |
| `src` | string | 是 | 来源 |
| `gen` | int ≥ 1 | 是 | 来源世代 |
| `seq` | int ≥ 0 | 是 | 世代内有界序号 |
| `dst` | string | 是 | 目的区域 |
| `business_id` | string | 是 | 业务 ID，用于识别重传 |
| `fingerprint` | string | 是 | 64 位小写十六进制 SHA-256，原请求指纹 |
| `created_at` | int ≥ 0 | 是 | 逻辑产生时刻（仅记录） |
| `predecessors` | array | 否 | 跨来源因果前置 `[{"src","gen","seq"}]` |

### 3.6 `gaps[]`（审计方签发的指令）

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `src` / `gen` / `seq` | — | 是 | — | 指令绑定的来源、世代、序号 |
| `kind` | `"PROVE"` / `"CANCEL"` | 是 | — | 关闭缺口 / 取消环中实体 |
| `reason` | string | 是 | — | 记录到回执里的原因 |
| `authority` | string | 否 | `"auditor"` | 签发方；必须等于受信任的 `auditor` |
| `arrival` | int ≥ 0 | 否 | 0 | 在时间线中的位置 |

---

## 4. 行为语义

### 4.1 接收窗口

每个 `(src, gen)` 一个窗口，`cursor` 是尚未有回执的最小序号。

- 消息落点 `seq < cursor`：已裁定，返回原回执（迟到的重发不会产生新回执）。
- 窗口右边界 `cursor + max_window - 1` 是**闭区间**；更远的消息整批记入
  `deferred`（`reason: WINDOW_FULL`）、不写入数据库，需要重投。
- 只裁定窗口头（`seq == cursor`）。序号更大的消息先缓冲，直到头被解决。
- 被拒绝的序号**仍占位**：`cursor` 前进，回执是 `status: REJECT`。

### 4.2 世代交接

新世代公告后即可预收消息（进缓冲区），但**发不出回执**，直到旧世代
`cursor > prev_end_seq`。这个门闸是窗口条件，不是依赖边：新世代的消息
永远不能反过来解开旧世代。

### 4.3 重传与业务 ID

- 相同 `(src, gen, seq)` 由不同网关转发 → 只有一条回执，`gateways` 累积所有转发者。
- 相同 `business_id`、相同 `fingerprint` → `status: DUPLICATE`，`depends_on` 指向原回执，
  `outcome` 继承原回执。
- 相同 `business_id`、不同 `fingerprint` → `status: REJECT`，`reason: ID_MISMATCH`，
  `reject_class: PROTOCOL`。
- 同一 fetch 内同一 `(src, gen, seq)` 绑定不同请求 → 整批拒绝（`E_SEQ_CONFLICT`）。
- 已入账的序号再次出现不同请求 → 整批拒绝，不改动已有回执。

### 4.4 因果前置

消息可引用其他来源（也可引用同源新世代）的回执。裁定顺序：

1. 世代门闸未开 → `WAIT_GATE`，不产生回执。
2. 前置尚未有回执 → `WAIT_DEP`，并**停在本源序号上**，不越过。
3. 前置已决且为拒绝 → 永久拒绝，`reason: DEPENDENCY_REJECTED`，
   `reject_class: DEPENDENCY`，`depends_on` 指向那条拒绝回执。
4. 前置已决且为允许 → 继续按规则与重传判定。

**预收状态不能顶替回执**：新世代消息即便已在缓冲区，前置方仍认为它未决。

补齐缺失前置后，下一个待决对象按 `(来源, 世代, 序号)` 字典序确定，
`inspect.next_to_adjudicate` 明确给出。

### 4.5 环

- 依赖图中处于同一强连通分量的缓冲消息标记为 `CYCLED`，窗口停住。
- `inspect.cycles` 列出所有环，`shortest_cycle` 是最短的一个（同长按字典序）；
  环按「成员数、成员坐标」排序，结果是确定的。
- 缺失（从未到达）的消息**不是**环的成员：它只能由 `PROVE` 关闭。
- 只有审计方 `CANCEL` 能解开环：被取消成员得到 `REJECT / CYCLE_CANCELLED`，
  其余成员按依赖正常连锁裁定（通常是 `DEPENDENCY_REJECTED`），
  `gap_results[].collateral` 列出这一连锁产生的回执。

### 4.6 审计 gap

- `PROVE`：关闭指定序号。空槽位 → 生成 `REJECT / GAP_CLOSED`；
  槽位里有缓冲的普通消息 → 该消息被拒绝（`GAP_CLOSED`，`gap_reason` 记录原因）；
  槽位已决 → 幂等返回原回执。
- 已关闭的缺口再次到达的消息返回**原回执**，不重复发放。
- `PROVE` 可以作用在环中实体上 —— 除非该实体尚未裁定且属于某个环：
  此时必须改用 `CANCEL`，否则报 `E_GAP_CYCLED`（见 4.5）。
- `CANCEL`：只对环中实体有效；已决的 `CANCEL` 幂等。
- 签发方不是 `auditor` → `E_GAP_AUTHORITY`，指令不生效。

### 4.7 事务与故障注入

`ingest` 一次调用 = 一个 `BEGIN IMMEDIATE` … `COMMIT`。裁定、窗口推进、
回执、规则版本、幂等键全部写在这一个事务里。

- `fail_after: N`：写第 N+1 条回执前中止，整个事务回滚，`committed: false`，
  数据库与调用前逐字节一致。
- `failpoint: "before_commit"`：算完但回滚。
- `failpoint: "after_commit"`：提交后报告 `committed: true`（模拟响应丢失）；
  重放同一案例不会重复发放。
- 并发 `ingest` 由 `BEGIN IMMEDIATE` 串行化；后到者在先到者提交后重新读状态，
  因此不会发放重复回执。

---

## 5. 输出字段

所有命令输出的顶层结构：

| 字段 | 类型 | 说明 |
|------|------|------|
| `command` | string / null | 执行的命令；解析失败时为 null |
| `ok` | bool | 成功为 true；失败时 `error` 存在 |
| `error` | object | `{code, path, detail}` |

### 5.1 回执对象（`receipts[]`，`simulate` 与 `dump` 共用）

| 字段 | 类型 | 说明 |
|------|------|------|
| `receipt` | int | 回执号，从 1 连续递增 |
| `src` / `gen` / `seq` | — | 被裁定消息的坐标 |
| `status` | `ACCEPT` / `REJECT` / `DUPLICATE` | 回执类别 |
| `outcome` | `ALLOW` / `DENY` | 最终效果；`DUPLICATE` 继承原回执 |
| `reason` | string | 见下方原因表 |
| `reject_class` | `PERMISSION` / `DEPENDENCY` / `PROTOCOL` / null | 拒绝分类 |
| `dst` | string / null | 目的区域（gap 生成的回执为 null） |
| `business_id` | string / null | 业务 ID |
| `fingerprint` | string / null | 原请求指纹 |
| `created_at` | int / null | 逻辑产生时刻 |
| `rule_id` | string / null | 裁定所用规则版本 id |
| `rule_epoch` | int / null | 该版本的 `effective_epoch` |
| `epoch` | int | 裁定时的逻辑时刻 |
| `batch` | string / null | 产生该回执的事件批 |
| `depends_on` | int / null | 依赖/重传/拒绝所指向的回执号 |
| `gateways` | array | 转发过该消息的所有网关 |
| `gap_authority` / `gap_reason` / `gap_kind` | string / null | 若由审计指令产生 |

`reason` 取值：

| `reason` | `status` | `outcome` | `reject_class` | 含义 |
|----------|----------|-----------|----------------|------|
| `ACCEPT` | ACCEPT | ALLOW | null | 规则允许 |
| `RETRANSMIT` | DUPLICATE | 继承 | null | 同业务 ID、同指纹的重传 |
| `NO_RULE` | REJECT | DENY | PERMISSION | 没有任何覆盖该目的地的规则 |
| `REVOKED` | REJECT | DENY | PERMISSION | 规则明确不允许该目的地 |
| `DEPENDENCY_REJECTED` | REJECT | DENY | DEPENDENCY | 前置被拒 |
| `ID_MISMATCH` | REJECT | DENY | PROTOCOL | 同业务 ID 绑定不同请求 |
| `GAP_CLOSED` | REJECT | DENY | PROTOCOL | 审计方证明的缺口 |
| `CYCLE_CANCELLED` | REJECT | DENY | PROTOCOL | 审计方取消环中实体 |

### 5.2 `simulate` / `ingest`

| 字段 | 类型 | 说明 |
|------|------|------|
| `durable` | bool | 是否落地 |
| `committed` | bool | 仅 `ingest`：事务是否提交 |
| `case_id` | string | |
| `announcements` | array | 本次新安装的公告 |
| `rules_installed` | array | 本次新安装的规则版本 id |
| `batches` | array | 每批的处理摘要（见下） |
| `gap_results` | array | 每条审计指令的结果 |
| `rejected` | array | 被整批/整指令拒绝的动作 `{kind, id, code, path, detail}` |
| `deferred` | array | 超出窗口而未被接收的消息 `{src, gen, seq, gateway, reason, window_left, window_right, batch}` |
| `receipts` | array | `simulate` 为完整回执对象；`ingest` 为本次新发放的回执号 |
| `new_receipts` | array | 仅 `ingest`：本次新发放的完整回执对象 |
| `counts` | object | `{ACCEPT, REJECT, DUPLICATE}` 全库计数 |
| `checksum` | string | 仅 `ingest`/`dump`：内容校验和 |
| `state` | object | 见 5.3 |
| `fault` | object | 故障注入时的结果 |

`batches[]` 摘要：

| 字段 | 说明 |
|------|------|
| `batch` / `arrival` / `epoch` | 批次坐标 |
| `applied` | 是否被接受（有 `deferred` 时为 false） |
| `replayed` | 是否为完全相同的重放 |
| `admitted` | 新进缓冲区的消息数 |
| `rules_installed` | 本批安装的规则 id |
| `receipts` | 本批产生的回执号 |
| `deduped` | 被识别为重复的消 `{src, gen, seq, gateway, receipt}` |
| `deferred` | 被窗口拒绝的消 `{src, gen, seq, gateway, reason, window_left, window_right}` |

`gap_results[]`：

| 字段 | 说明 |
|------|------|
| `gap` | `{src, gen, seq, kind, reason, authority}` |
| `applied` / `idempotent` / `cancelled_cycle` | 状态 |
| `receipt` | 该序号最终的回执号（可能为 null） |
| `collateral` | 指令连锁产生的回执摘要 |

### 5.3 `state`（`simulate` / `ingest`；`dump` / `inspect` 平铺）

| 字段 | 说明 |
|------|------|
| `epoch` | 当前逻辑时刻 |
| `next_receipt` | 下一条回执号 |
| `windows` | `[{src, gen, start_seq, prev_end_seq, cursor, gate_open}]` |
| `pending` | 缓冲消息 `[{src, gen, seq, state, wait, business_id, dst, gateways, blocked_on, gap}]` |
| `next_to_adjudicate` | 下一个待决对象及 `wait` / `blocked_on` |
| `cycles` | `[{length, members:[{src,gen,seq}], edges:[{from,to}]}]` |
| `rules` | `[{id, effective_epoch, precedence, routes, active, origin_batch}]` |
| `gaps` | `[{src, gen, seq, kind, reason, authority}]` |
| `receipt_count` | 回执总数 |

`wait` 取值：`WAIT_GATE`（世代门闸）、`WAIT_DEP`（因果前置）、`WAIT_DUP`
（等待同 ID 原消息）、`null`（未在等待，只是排在窗口头之后）。
`state` 取值：`PENDING` / `CYCLED`（`RESOLVED` 的不出现在 `pending` 里）。

### 5.4 `dump`

`dump` 平铺 `state` 的字段，并额外提供：

| 字段 | 说明 |
|------|------|
| `results` | 每条结果的精简视图 `{src, gen, seq, receipt, status, reason, reject_class, rule_id, rule_epoch}` |
| `receipts` | 完整回执对象（含规则版本） |
| `checksum` | 内容校验和 |

### 5.5 `inspect`

| 字段 | 说明 |
|------|------|
| `windows` | 同上 |
| `gated_windows` | `gate_open == false` 的世代 |
| `pending` | 同上 |
| `next_to_adjudicate` | 同上 |
| `cycles` / `cycle_count` / `shortest_cycle` | 全部环 / 数量 / 最短环 |
| `blocked` | 是否有缓冲消息 |
| `rules` / `gaps` / `next_receipt` | 同上 |

### 5.6 `verify`

| 字段 | 说明 |
|------|------|
| `ok` | 数据库是否健康 |
| `report.problems` | `[{code, detail}]` |
| `report.checksum` / `stored_checksum` | 重算 / 存储的内容校验和 |
| `report.receipts` / `windows` | 计数 |

`report.problems[].code`：`E_DB_FORMAT`、`E_DB_CHECKSUM`、`E_DB_CURSOR`、
`E_DB_ORPHAN_RECEIPT`、`E_DB_RECEIPT_GAP`、`E_DB_RECEIPT_COUNTER`。

---

## 6. 错误码

| 码 | 退出码 | 含义 |
|----|--------|------|
| `E_USAGE` | 2 | 命令行用法错误 |
| `E_JSON_INVALID` | 3 | 非法 JSON |
| `E_JSON_DUPLICATE_KEY` | 3 | JSON 对象键重复 |
| `E_INPUT_UNREADABLE` | 3 | 案例文件读不到 |
| `E_INPUT_ENCODING` | 3 | 不是 UTF-8 |
| `E_TYPE_EXPECTED_OBJECT` / `_ARRAY` / `_INTEGER` / `_BOOLEAN` / `_STRING` | 3 | 类型错误 |
| `E_FIELD_MISSING` / `E_FIELD_UNKNOWN` | 3 | 字段缺失 / 未知字段 |
| `E_VALUE_EMPTY` / `E_VALUE_TOO_LONG` / `E_VALUE_OUT_OF_RANGE` / `E_VALUE_INVALID` | 3 | 取值错误 |
| `E_FINGERPRINT_INVALID` | 3 | 指纹不是 64 位小写十六进制 |
| `E_BATCH_EMPTY` / `E_BATCH_SIZE` | 3 | 批为空 / 超过 `max_batch` 或 `batch_size` |
| `E_DUP_RULE` / `E_DUP_ROUTE` / `E_DUP_BATCH` / `E_DUP_ANNOUNCEMENT` | 3 | 重复定义 |
| `E_GAP_CONFLICT` | 3 | 同一 arrival 上对同一序号给出矛盾指令 |
| `E_CASE_MISMATCH` | 3 | 案例与数据库中已存案例不符 |
| `E_SEQ_CONFLICT` | 3 | 同一序号绑定不同请求（拒绝整批） |
| `E_ID_CONFLICT` | 3 | 同一批内同一业务 ID 绑定不同请求（拒绝整批） |
| `E_GEN_NOT_ANNOUNCED` / `E_SEQ_BEFORE_START` | 3 | 世代未公告 / 序号低于起始 |
| `E_BATCH_REPLAY_CONFLICT` | 3 | 同一批 id 以不同内容重放 |
| `E_RULE_VERSION_CONFLICT` / `E_RULE_AMBIGUOUS` | 3 | 规则版本冲突 / 平局歧义 |
| `E_ANNOUNCEMENT_CONFLICT` / `_PREV_MISSING` / `_BOUNDS` | 3 | 公告冲突 / 前世代缺失 / 边界非法 |
| `E_GAP_AUTHORITY` / `E_GAP_CYCLED` / `E_GAP_NOT_CYCLED` / `E_GAP_BELOW_CURSOR` | 3 | 审计指令无效 |
| `E_DB_REQUIRED` / `E_DB_MISSING` / `E_DB_UNOPENABLE` / `E_DB_NOT_A_DATABASE` | 4 | 数据库不可用 |
| `E_DB_SCHEMA` / `E_DB_CHECKSUM` / `E_DB_WRITE` / `E_DB_LOCKED` | 4 | 数据库结构 / 校验 / 写入 / 锁 |

整批层面的 `E_SEQ_CONFLICT`、`E_ID_CONFLICT`、`E_GEN_NOT_ANNOUNCED`、
`E_SEQ_BEFORE_START`、`E_BATCH_REPLAY_CONFLICT`、`E_RULE_VERSION_CONFLICT`、
`E_RULE_AMBIGUOUS` 不会让整个运行失败：该批被**整体**丢弃（不安装规则、
不推进 epoch、不缓冲任何消息）并记录在 `rejected` 中。审计指令的语义错误
同样记录在 `rejected`。公告与顶层规则的问题则中止整个运行，因为后续裁定
会建立在并不存在的策略上。

---

## 7. 复现样例

所有样例都在 `samples/`，可直接运行；`samples/build_samples.py` 是它们的生成脚本。

### 7.1 双网关重发（`samples/01_two_gateways.json`）

```bash
python -m solution simulate samples/01_two_gateways.json
```

期望：序号 100 被两个网关各转发一次，但只有 `receipt: 1`；
其 `gateways` 为 `["relay-a", "relay-b"]`；101 被规则撤销（`REVOKED`）；
102 允许。`counts` = `{"ACCEPT": 2, "DUPLICATE": 0, "REJECT": 1}`。

### 7.2 换代卡在缺口（`samples/02_generation_gap.json`）

```bash
rm -f demo2.sqlite
python -m solution ingest  samples/02_generation_gap.json --db demo2.sqlite
python -m solution dump    samples/02_generation_gap.json --db demo2.sqlite
```

新世代序号 1 在第二批就到达并被缓冲；在 3、4 被审计 `PROVE` 关闭之前，
它拿不到回执。最终回执顺序为 1、2、3（gap 3）、4（gap 4）、5（新世代 1）。

把 `gaps` 删掉再跑一次，`inspect` 会显示新世代窗口 `gate_open: false`、
缓冲消息 `wait: WAIT_GATE`：

```bash
python -m solution inspect samples/02_generation_gap.json --db demo2.sqlite
```

### 7.3 环与解除（`samples/03_cycle_and_release.json`）

```bash
python -m solution simulate samples/03_cycle_and_release.json
```

去掉文件里的 `gaps` 后再跑 `simulate`，可以看到两个窗口都停在 1，
`state.cycles` 返回长度为 2 的最短环；带 `CANCEL` 时被取消成员
`CYCLE_CANCELLED`，对端 `DEPENDENCY_REJECTED`，同源的后续消息恢复处理。

### 7.4 策略屏障（`samples/04_policy_barrier.json`）

```bash
python -m solution simulate samples/04_policy_barrier.json
```

`n-1` 在 `zone-2` 上等待 `gw-south/1/1`；它的前置恰好在 `epoch=5` 的那个事务里
才就绪，因此解封后读到的必须是 `fleet-default` 的 epoch 5 版本（`zone-2` 已撤销）：
`n-1` 与 `n-2` 都是 `REVOKED`，`rule_epoch: 5`，而不是旧版本给出的 ACCEPT。
把 emergency 那批的 `rules` 删掉，两条消息就会按 epoch 0 版本放行——这正是
「更新后不得读取旧规则裁定后来从等待态解封的消息」要防止的差别。

### 7.5 窗口边界与业务 ID（`samples/05_window_and_ids.json`）

```bash
python -m solution simulate samples/05_window_and_ids.json
```

`max_window: 3` 时，首批的 13 落在 `[10,12]` 之外，记录为 `deferred`；
第三批重投 13 成功。14 复用了 10 的业务 ID 但指纹不同，
得到 `ID_MISMATCH`。

### 7.6 故障恢复（`samples/06_fault_recovery*.json`）

```bash
rm -f demo6.sqlite
python -m solution ingest samples/06_fault_recovery.json --db demo6.sqlite
# -> committed: false, 数据库里 0 条回执
python -m solution dump   samples/06_fault_recovery.json --db demo6.sqlite
# -> receipts: []
python -m solution ingest samples/06_fault_recovery_retry.json --db demo6.sqlite
# -> 回执 1,2,3 各发放一次
python -m solution ingest samples/06_fault_recovery_retry.json --db demo6.sqlite
# -> receipts: []，幂等
```

### 7.7 结构错误定位（`samples/07_bad_field.json`）

```bash
python -m solution simulate samples/07_bad_field.json
```

```json
{
  "command": "simulate",
  "error": {
    "code": "E_FINGERPRINT_INVALID",
    "detail": "fingerprint must be 64 lower-case hex characters",
    "path": "batches/0/events/1/fingerprint"
  },
  "ok": false
}
```

退出码为 3。

---

## 8. 测试

```bash
python -m unittest discover -s tests -v
```

| 文件 | 覆盖 |
|------|------|
| `tests/test_validation.py` | 结构校验、错误路径、CLI 契约、输出确定性 |
| `tests/test_windows.py` | 窗口左右边界、乱序、拒绝占位、双网关重发、同 ID 异内容、换代门闸、gap |
| `tests/test_dependencies.py` | 跨来源前置、依赖拒绝 vs 权限拒绝、预收不顶替回执、策略屏障与规则版本 |
| `tests/test_cycles.py` | 两源死锁、最短环、CANCEL 解除与连锁、PROVE 不能解环、幂等 |
| `tests/test_races.py` | 重传与原消息同批、gap 与前置竞态、依赖拒绝传导 |
| `tests/test_storage.py` | 单事务、故障回滚、重启不重复、dump 规则版本、verify |
| `tests/test_concurrency.py` | 真线程并发 ingest、同批竞态、两网关同请求竞态 |
| `tests/test_differential.py` | 与独立小状态机对拍：随机案例、拆分 ingest、故障后复原 |

`tests/reference_model.py` 是独立实现的小状态机：它把消息放在列表里、
每次从零重算结果，而不是像引擎那样移动游标。两者的输出逐个坐标比对。

---

## 9. 目录结构

```
solution/
  __main__.py      python -m solution 入口
  cli.py           命令解析、输出组装、退出码
  validation.py    案例文件的严格结构校验（带字段路径）
  engine.py        纯状态机：窗口、世代、规则版本、依赖、环、审计指令
  storage.py       SQLite 持久化、单事务提交、校验和
  jsonio.py        严格 JSON 读写（拒绝重复键与 NaN）
  errors.py        SimError
samples/           可直接运行的虚构事件批
tests/             断言测试与独立对拍模型
```

## 10. 离线声明

- 不导入 `socket`、`ssl`、`urllib`、`http`、`requests`、`subprocess`。
- 不读取系统时间：所有顺序由案例文件里的 `arrival`、`epoch`、`created_at` 决定。
- 随机性只出现在测试的固定种子生成器里，不进入被测代码。
- 持久状态只写 `--db` 指定的 SQLite 文件。
