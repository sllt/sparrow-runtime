# PostgreSQL Source / Sink / Lookup

PostgreSQL 以三种方式接入，均需要 build feature `postgres`（`sparrow-server/postgres`）：

- **Source**（`source.kind: "postgres"`）：周期执行一条查询，按跟踪列（tracking column）增量读取。
- **Sink**（`sink.kind: "postgres"`）：`INSERT` 或 `INSERT ... ON CONFLICT`（UPSERT），每个上游批次一个事务，`COMMIT` 成功后才回执。
- **外部 Lookup provider**（`external_lookups.<name>.postgres`）：按 key 读一张表，与 HTTP / Redis Lookup 共用同一个 runtime `ExternalLookup` 接口、Lookup 算子、缓存、选项和错误策略。

不支持逻辑复制 / CDC。

未开启 feature 的构建在解析 spec 时对上述三者返回 `FeatureUnavailable`，不会忽略配置。

## 客户端

使用 `tokio-postgres`（固定版本 `=0.7.18`，不依赖 libpq），TLS 使用仓库已有的 rustls。下列行为来自对该版本源码的阅读，并由本仓库代码补足：

- **只用扩展查询协议**：配置的查询只能是一条语句，`SELECT 1; DROP TABLE t` 在 prepare 时被服务器拒绝。
- **消息大小**：tokio-postgres 的解码器对单条后端消息没有上限，会一直缓冲直到整条消息到达。本仓库在 socket 与客户端之间加了一层读取包装：解析每条后端消息的头部，长度超过该连接的上限（行上限 + 64 KiB）时直接让连接失败，不缓冲消息体。
- **取消**：丢弃 tokio-postgres 的查询 future 不会让服务器停止执行（连接任务会继续读完回复）。所以凡是因超时或停止而放弃的语句（包括连接后的 prepare 和目录查询），都会另开一个连接发送 cancel request（受 `connect_timeout_ms` 限制），并关闭原连接；服务器随之回滚未提交的事务。
- **认证**：SCRAM-SHA-256 / MD5 / cleartext 由客户端处理。不使用 SCRAM channel binding（`channel_binding=disable`），服务器身份由 `verify-full` 保证。因为客户端在服务器要求时会发送明文密码，所以密码只允许在 `verify-full` 下使用。

## 连接

三种用法共用同一组连接字段：

```json
"url": "postgresql://db.example:5432/app",
"user": "sparrow_writer",
"password_secret": "pg_pw",
"sslmode": "verify-full",
"ca_pem": "-----BEGIN CERTIFICATE-----\n...",
"connect_timeout_ms": 5000
```

**URL**

- 只接受 `postgresql://` 或 `postgres://`，路径为 `/<dbname>`（1..=63 字节，可百分号编码），默认端口 5432。
- 拒绝 userinfo、query 和 fragment：角色名是 `user` 字段，密码只能是 secret 引用，TLS 由 `sslmode` 决定。

**`sslmode`**（只支持两种）

| 值 | 行为 |
|---|---|
| `verify-full`（默认） | 必须 TLS；服务器拒绝 SSLRequest 时连接失败；校验证书链（内置根证书，或 `ca_pem` 替换之）和主机名 |
| `disable` | 明文 TCP；不能与 `password_secret` 同时使用（`PolicyDenied`） |

`prefer` / `allow` 可能回落到明文，`require` / `verify-ca` 不校验主机名，四者都报 `PolicyDenied`。`ca_pem` 只能与 `verify-full` 一起使用。

**其他**

- secret 不能为空且不超过 4 KiB，否则报 `SecretMissing`。
- host:port 经过 `TargetPolicy` 白名单检查，与其他出站连接相同。
- `connect_timeout_ms`（100..=60000，默认 5000）覆盖 TCP、TLS、启动和认证。
- 认证被拒（SQLSTATE 28xxx）报 `PolicyDenied`，数据库不存在（3D000）报 `InvalidArgument`，均不重试。
- 连接的 `application_name` 为 `sparrow`。Debug 输出不包含密码。

## 类型映射

映射是显式的。列类型在 prepare 时（Source）或连接后读 `pg_attribute`（Sink / Lookup）确定，不在运行时猜测。

| PostgreSQL | Sparrow 字段类型 |
|---|---|
| int2, int4, int8 | Int64（写入时检查范围） |
| float4, float8 | Float64（只允许有限值） |
| numeric | Utf8（精确的十进制文本）或 Float64（最接近值，有损） |
| text, varchar, bpchar, name | Utf8 |
| bool | Bool |
| timestamp, timestamptz | TimestampMicrosUTC（timestamp 按 UTC 读写；`±infinity` 拒绝） |
| json, jsonb | Utf8（JSON 文本；写入前在客户端校验为合法 JSON，jsonb 拒绝 `\u0000`） |
| bytea | Bytes |

其他类型（域、枚举、数组、uuid、date、time、interval、money 等）一律拒绝（`InvalidSchema`）。需要时在查询里显式转换，例如 `SELECT id::text ...`。

写入时值不符合列类型（超出 int2/int4 范围、文本含 NUL、非法 JSON、非有限 float4、写 NULL 到 NOT NULL 列、冲突键为 NULL）的行会被丢弃并计数，见 Sink 一节。

## Source

```json
"source": {"kind": "postgres", "inbox_capacity": 16, "postgres": {
  "url": "postgresql://db.example/app", "user": "reader", "password_secret": "pg_pw",
  "query": "SELECT id, device_id, v, updated_at FROM readings",
  "tracking_column": "id",
  "fetch_rows": 200,
  "poll_interval_ms": 1000,
  "query_timeout_ms": 30000,
  "start_after": 1000
}}
```

`query` 必须是一条可以作为子查询的 SELECT（1..=65536 字节）。它被包装成：

```sql
SELECT <字段，超大行为 NULL>, z.o, q."<tracking>"
FROM (<query>) AS q
CROSS JOIN LATERAL (SELECT <行的二进制大小> > $2 AS o) AS z
WHERE q."<tracking>" IS NOT NULL AND q."<tracking>" > $3   -- 第一次查询没有 "> $3"
ORDER BY q."<tracking>" LIMIT $1
```

所有值都是绑定参数，只有经过引用的标识符会拼进 SQL。每一页在 `BEGIN READ ONLY ... COMMIT` 中执行。

**字段与跟踪列**

- Source 的字段就是 pipeline stream schema 的字段，按名字对应查询的列，类型须符合上面的映射，否则在连接时报 `InvalidSchema` 并停止。
- 跟踪列必须是 int2 / int4 / int8 / timestamp / timestamptz，且是查询的一列（可以不在 schema 中）。跟踪值为 NULL 的行永远不会读到。
- 行按跟踪值升序交付；跟踪值相同的行之间的顺序由服务器决定。

**进度**

- 一页的所有行都进入 job（或计为丢弃）之后，才把“上一个跟踪值”推进到本页的最大值。
- 页满（`fetch_rows` 行）时，本页末尾与最大跟踪值相同的行不交付，留给下一次查询，所以相同跟踪值的行不会被拆到两页。如果一整页的跟踪值全部相同，Source 无法前进，报 `BoundExceeded` 并停止（需要调大 `fetch_rows`）。
- 页满时立即查询下一页，否则等待 `poll_interval_ms`（100..=86400000，默认 1000）。
- `start_after`：从严格大于该值处开始；timestamp 跟踪列以 1970-01-01 UTC 起的微秒表示。
- 当前跟踪值发布在诊断中（`postgres_source.tracking_value`）。

**为什么只能是 live（不能作为 checkpoint 回放点）**

Source 是 `live_best_effort` / `restart_fresh`，拒绝 restore、checkpoint 和 aligned（包括 graph Source）。原因：跟踪值不能标识表的一个完整前缀。并发事务、序列按会话缓存、时钟偏差都可能让较小的跟踪值在较大的值之后才提交，这些行会被跳过；把跟踪值写进 checkpoint 并不能让重放补回它们。重启时从 `start_after`（或最小值）重新开始，不会从上次的跟踪值继续。

**内存与边界**

- `fetch_rows`（1..=10000）：省略时取能放进 job reservation 一半的最大页（不超过 1000 行），按 job 的行上限计算；显式值只检查、不下调，放不下时报 `BoundExceeded`。校验阶段按最小（compact）kernel 检查（行上限 64 KiB、reservation 4 MiB，即最多 28 行）。
- 每次查询前，先按 `fetch_rows × (行上限 + 256) + 64 KiB` 从 job reservation 预扣整页额度，额度不足时等待重试（`budget_waits`）。另有 128 KiB 连接额度。
- 服务器端计算每行的二进制大小，超过行上限的行只回传标志位，不回传值，计 `dropped_oversize`。
- 解码失败的行计 `dropped_bad`；`fail_on_decode` 为 true 时 Source 报 `CodecViolation` 并停止。
- inbox 字节（`postgres.inbox_bytes`，默认 256 KiB）与条数（`source.inbox_capacity`）有界；满时等待（`backpressure_waits`），不丢行。

**错误与重连**

- 连接失败、连接中断、超时（`query_timeout_ms`，100..=300000，默认 30000）、页额度暂时不足、执行时的服务器错误（如查询中的除零、序列化失败）：断开并以退避重连（从 `poll_interval_ms` 起翻倍，上限 60 秒），计 `query_failures` / `timeouts`。
- 认证失败、查询语法 / 权限 / 对象不存在（SQLSTATE 42xxx 等）、形状不符：报错并停止 job，不重试。

**停止**：取消时丢弃在途语句，向服务器发送 cancel request 并关闭连接。

## Sink

```json
"sink": {"kind": "postgres", "outbox_capacity": 16, "postgres": {
  "url": "postgresql://db.example/app", "user": "writer", "password_secret": "pg_pw",
  "schema_name": "public", "table": "readings_latest",
  "columns": ["device_id", "v", "updated_at"],
  "mode": "upsert", "conflict_key": ["device_id"], "update_columns": ["v", "updated_at"],
  "chunk_rows": 1000, "chunk_bytes": 524288,
  "timeout_ms": 10000, "max_retries": 3, "retry_initial_ms": 100, "retry_max_ms": 5000,
  "flush_timeout_ms": 5000
}}
```

**语句**

```sql
INSERT INTO "public"."readings_latest" ("device_id", "v", "updated_at")
SELECT * FROM ROWS FROM (unnest($1::text[]), unnest($2::float8[]), unnest($3::timestamptz[]))
[ON CONFLICT ("device_id") DO UPDATE SET "v" = EXCLUDED."v", ... | DO NOTHING]
```

- 每列一个二进制数组参数，所以一条 prepare 过的语句适用于任意行数；numeric 列以 text 数组传送再转换，保留精确值。
- `columns` 省略时写 schema 的全部字段；字段名与列名相同。
- 连接后从 `pg_attribute` 读表的列类型，并与字段类型对照（不符报错并停止 job）。
- 一个上游批次一个事务。批次按 `chunk_rows`（1..=10000，默认 1000）/ `chunk_bytes`（4096..=16 MiB，默认 512 KiB）拆成多条语句，都在同一个事务里。
- `mode: "upsert"` 必须给出 `conflict_key`（1..=8 列，须为表上的唯一约束，不能是浮点列）和 `update_columns`（`[]` 表示 `DO NOTHING`）。同一批次内重复的冲突键会开始一条新语句，保证按顺序生效（否则服务器会报 “ON CONFLICT DO UPDATE command cannot affect row a second time”）。

**回执与重试**

- `COMMIT` 成功后批次才回执。
- `upsert`：重新应用同一批次结果相同，所以任何失败（包括 `COMMIT` 已发送但结果未知）都重试。
- `insert`：`COMMIT` 发送之前的失败（服务器报错，或连接断开导致服务器中止事务）会重试；`COMMIT` 已发送之后连接断开或超时，结果未知，**不重试**（计 `unknown_outcome`，批次失败），以免重复插入。
- 重试采用有上限的抖动退避（`retry_initial_ms` 10..=60000、`retry_max_ms` 100..=300000、`max_retries` 0..=20）；`timeout_ms`（100..=300000）限制一次事务尝试。

**SQLSTATE 分类**

| 类别 | 处理 |
|---|---|
| 08、40、53、57P01/02/03、57014、55P03 | 可重试 |
| 21、22、23（数据错误，如违反唯一约束） | 批次失败，不重试 |
| 28、42、3D、3F、0A、2B（认证、权限、表 / 列不存在、不支持） | Sink 停止，job 失败（`postgres_sink_fatal`） |
| 其他 | 批次失败 |

**坏行**：客户端检查不通过的行被丢弃（`dropped_bad`，超过 `chunk_bytes` 的行计 `dropped_oversize`），该批次的其余行照常写入并提交，但批次回执为失败。

**内存**：连接 128 KiB + 2 × `chunk_bytes` 的编码缓冲（`DO UPDATE` 另加 `chunk_rows × 64 + chunk_bytes` 用于记录冲突键）+ 1 KiB，在绑定时一次性从 job reservation 预扣，须 ≤ reservation 的一半；算术均经过溢出检查。默认值在最小（compact）预算下可用。

**停止**：第一次取消开始一个 `flush_timeout_ms`（10..=60000，默认 5000）截止时间，覆盖在途批次（及其重试）和排队批次。到期时仍在执行的语句收到 cancel request，连接被关闭（服务器回滚事务），剩余批次计 `discarded_on_close`。

**语义**：`live_best_effort` / `restart_fresh`，拒绝 restore、checkpoint 和 aligned（包括 graph Sink）。

## Lookup

```json
"external_lookups": {
  "devices": {
    "postgres": {"url": "postgresql://db.example/app", "user": "reader",
                 "password_secret": "pg_pw", "schema_name": "public", "table": "devices"},
    "fields": [{"name":"tenant","type":"utf8","nullable":false},
               {"name":"device_id","type":"int64","nullable":false},
               {"name":"threshold","type":"int64","nullable":true}],
    "keys": ["tenant", "device_id"],
    "options": {"max_inflight": 2, "batch_keys": 3, "timeout_ms": 500,
                "cache_ttl_ms": 1000, "on_error": "fail"}
  }
}
```

每个外部 Lookup 必须且只能声明 `url`（HTTP）、`redis` 或 `postgres` 中的一个。`header_secret` 只适用于 HTTP。

**查询**：一批 key 一条语句。

- 单个 key 列：`... WHERE t."k" = ANY($1::<类型>[])`。
- 多个 key 列：`... JOIN ROWS FROM (unnest($1::<t1>[]), unnest($2::<t2>[])) AS u(k0, k1) ON t."k1" = u.k0 AND ...`。
- 都带 `LIMIT <不同 key 数 + 1>`：一个 key 匹配到两行时报 `CodecViolation`（key 列不唯一），而不会读回无界结果。

**字段与 key**

- 字段读同名列，类型须符合映射。key 列须为 int2/int4/int8、text/varchar/name、bool、timestamp(tz) 或 bytea（不能是浮点）。
- 不可能存在于该列的 key（超出 int2/int4 范围、文本含 NUL、timestamp 超范围）直接视为未命中，不发请求。
- 超过 64 KiB 的行由服务器标记，报 `BoundExceeded`。

**批量与连接池**

- `options.batch_keys`（1..=64）大于 1 时，一次请求最多带这么多个未命中的 key。
- `max_inflight` 也是连接池大小，每个连接同一时刻只有一条语句。
- `timeout_ms` 覆盖等待连接池、建连和查询。超时或取消时，在途语句收到 cancel request，连接被关闭。
- 复用的空闲连接已断开时，在同一截止时间内换新连接重试一次（只读查询，重试安全）。

**错误分类**：`on_error` 只对 transport 类错误生效。

| 情况 | 错误码 | 受 `on_error` 影响 |
|---|---|---|
| 超时、连接失败或断开、一般服务器错误 | `JobFailed` | 是 |
| 认证失败、权限不足（42501） | `PolicyDenied` | 否 |
| 表 / 列不存在等 42xxx、类型不符 | `InvalidArgument` / `InvalidSchema` | 否 |
| key 不唯一、解码失败、超过 64 KiB | `CodecViolation` / `BoundExceeded` | 否 |

**内存**

- provider 声明的 scratch 为每个 key 192 KiB：一页结果最多“不同 key 数 + 1”行、每行 ≤ 64 KiB，加一条消息的余量（64 KiB），按 1 个 key 的最坏情况折算。
- 算子另外为每个 key 计 64 KiB + 1 KiB。`max_inflight × batch_keys` 个 key 的窗口须 ≤ job reservation 的一半；4 MiB 预算下最多 7 个 key，超出时在启动时拒绝。
- 连接池的连接缓冲不计入 job 额度（与 HTTP / Redis Lookup 相同）。

## 诊断与指标

- pipeline status：`postgres_source`（rows、queries、dropped_bad、dropped_oversize、backpressure_waits、budget_waits、connects、connect_failures、query_failures、timeouts、tracking_value、inbox_items、inbox_bytes）和 `postgres_sink`（rows、transactions、statements、batch_errors、dropped_bad、dropped_oversize、budget_waits、retries、connects、connect_failures、timeouts、unknown_outcome、discarded_on_close、fatal）。
- `/metrics` 中同名的 `postgres_source_*` / `postgres_sink_*` 计数。
- Lookup 计数在 `lookup_runtime.external`。

## 测试

- 单元测试：标识符引用、sslmode、URL、secret 与策略、SQLSTATE 分类、消息长度包装、类型编解码（含 numeric 和数组参数的线格式）、语句生成、配置边界（含 `usize::MAX`）。
- 真实 PostgreSQL（可选，`SPARROW_POSTGRES_BIN` 指向 PostgreSQL 16.x 的 `bin` 目录，需带 OpenSSL 构建）：TLS + SCRAM、认证失败、sslmode 拒绝、超大消息、Sink 类型 / UPSERT 幂等 / 坏行 / schema 与权限失败 / COMMIT 结果未知 / 重连 / 停止取消在途语句、Source 进度 / 相同跟踪值 / 超大行 / 重连 / 停止取消、Lookup 批量 / 未命中 / 错误 / 超时取消 / 连接池恢复，以及经 Supervisor 的 Source → Lookup → UPSERT Sink 端到端。

## 不承诺

- 不支持 CDC、`COPY`、多语句查询、服务器端游标；Source 不是 checkpoint 回放点，会跳过晚提交的较小跟踪值。
- 不使用 SCRAM channel binding；不支持客户端证书认证。
- 未测试 PostgreSQL 16 以外的版本，也未测试连接池中间件（PgBouncer 等）。
- Lookup 连接池的连接缓冲和 TLS 内部缓冲不计入 job 额度。
