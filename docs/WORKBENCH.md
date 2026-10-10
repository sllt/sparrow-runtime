# Sparrow 运维工作台（K5.1–K5.2）

K5.1 交付服务端角色授权、可选静态工作台和只读运维页面。本文只描述已实现并有测试覆盖的行为；后续 K5.2～K5.6 的能力（草稿、发布、Graph Designer、Preview、处置向导、导入导出）尚未实现。

## 鉴权

两种互斥模式，启动时选择：

| 模式 | 启动方式 | 身份 |
| --- | --- | --- |
| 旧单 token | `SPARROW_TOKEN`（或 `--token`） | 唯一身份 `token`，角色 `admin`（与 K5 之前行为一致） |
| 角色文件 | `--auth-file PATH` 或 `SPARROW_AUTH_FILE` | 文件中列出的 actor/role |

同时给出角色文件和 `SPARROW_TOKEN` 会拒绝启动，不保留隐藏的旧 admin token；需要兼容身份时请在文件里明确列出。

角色文件格式（只保存 token 的 SHA-256，不保存明文）：

```json
{"version":1,"principals":[
  {"actor":"ops-wang","role":"viewer","token_sha256":"<64 位小写 hex>"},
  {"actor":"ops-li","role":"operator","token_sha256":"..."},
  {"actor":"admin-ma","role":"admin","token_sha256":"..."}
]}
```

生成摘要：`printf %s "$TOKEN" | sha256sum`。以下任一情况启动失败（fail closed）：文件不存在或不是普通文件、权限对 group/other 开放（需 `chmod 600`）、超过 64 KiB、JSON 无效或含未知字段、`version≠1`、条目为 0 或超过 64、未知角色、actor 不合法（`[A-Za-z0-9-_.@]`，≤64）、重复 actor、重复凭据摘要、摘要格式错误。

鉴权/授权失败都不写 SQLite（保留原防放大设计）。审计 actor 使用鉴权得到的真实身份（插件、退役等路径不再写死 `"token"`）；`validate` 不再把 SQL 文本记入审计 target。

## 权限矩阵

授权在服务端按“方法 + 路由”判定（`crates/sparrow-server/src/auth.rs` 的 `ACTIONS` 表）；未列出的路由要求 admin。

| 角色 | 允许 |
| --- | --- |
| viewer | `GET /v1/auth/me`、`/v1/capabilities`、`/v1/metrics`、`/v1/pipelines`（名字）、`/v1/pipelines/{n}/status`（安全投影）、`/v1/pipelines/{n}/diagnose`、`/v1/audit`（不含 detail） |
| operator | viewer 全部 + validate/explain/graphs/test/query、Stream 读写、参考表读/发布/mutate、pipeline 读取/发布/start/stop/checkpoint、checkpoint 清单、outbox/input-DLQ 状态、恢复操作只读、插件列表/引用、demo |
| admin | 全部，包括 secret、allowlist、插件安装/启停/卸载/attest、retire、表 GC/rollback、restore、kill、outbox 条目/命令、DLQ 条目/purge、recovery preview/execute/finish/abort |

越权返回 `403`，`error.code=policy_denied`。safe-mode/draining 的既有限制对 admin 同样生效。

viewer 的 status 是服务端生成的安全投影（与诊断包相同的 allowlist）：不含 `spec`、SQL、目标地址、SecretRef、原始错误文本；`actual.last_error` 只以 `has_error` 布尔值呈现，响应带 `"projection":"viewer_safe"`。

`GET /v1/auth/me` 返回 `actor`、`role`、`allowed_actions`、`auth_mode`、`safe_mode`、`draining`、`status_projection`，不返回 token 或摘要。

所有经过管理中间件的响应（含 401/403）带 `Cache-Control: no-store`。

## 静态工作台 `--ui-dir`

- 未配置时行为与之前完全相同（`/ui/*` 与其他未知路径一样需要鉴权）。
- 配置后，启动时扫描目录生成内存清单：只收普通文件，跳过符号链接和以 `.` 开头的名字，最多 2048 个文件 / 64 MiB / 8 层；缺少 `index.html` 拒绝启动。请求按清单键精确查找，不存在目录遍历、目录列表或跟随链接。
- 页面固定在 `/ui/`，`/ui` 308 跳转。SPA fallback 只用于 `/ui/` 下不含扩展名的路径；缺失的资源文件返回 404。`/`、`/v1/health` 和 API（含 401/404）不受影响。
- 静态资源匿名可读，安全头：`Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'`、`X-Content-Type-Options: nosniff`、`X-Frame-Options: DENY`、`Referrer-Policy: no-referrer`、`Cross-Origin-Opener-Policy: same-origin`。不允许 `unsafe-eval` 或远端来源；`style-src 'unsafe-inline'` 为组件内联样式保留。
- API 没有开放 CORS；页面与 `/v1` 同源。远程访问请经已有 HTTPS 反向代理。

## 前端（`web/`）

React 19 + TypeScript + Vite 6，单一 `package-lock.json`，Node 20.19.2 / npm 9.2.0（`.nvmrc`、`engines`、`engine-strict`）。核心 `cargo build` 不调用 npm。

```bash
cd web && npm ci && npm run build          # 产出 web/dist
SPARROW_AUTH_FILE=/etc/sparrow/auth.json sparrow-server --ui-dir web/dist
# 浏览器打开 http://127.0.0.1:43180/ui/
```

开发：`npm run dev`（Vite 代理 `/v1` 到 `SPARROW_DEV_API`，默认 `http://127.0.0.1:43180`，仅限本地开发）。

行为约定：

- token 只保存在内存中的 API client 闭包里；不进 URL、localStorage/sessionStorage、日志或错误文本。刷新需重新登录；注销或 401 丢弃 client 并卸载所有轮询（中止在途请求）。只持久化主题偏好。
- 所有响应先读文本再经 `lossless-json` 解析：超出安全整数范围的整数保留为精确文本，浮点字面量原样回写，数组顺序和未知字段保留。
- 轮询：当前页面约 5 秒（总览 10 秒，最多读取 50 条流水线的 status、并发 4）；单请求不重叠；页面隐藏时暂停；失败/429 指数退避（429 起点加倍，上限 60 秒）；401 停止并要求重新登录；导航/注销取消。
- 状态区分：`运行中` 仅在 status 新鲜、`actual=running` 且有实时观测、且没有失败类诊断原因时显示；`数据过期`、`观测不可用`、`状态未知`、`无权限` 分别显示。旧数据保留时有醒目的“已过期”横幅。
- 速率由本 attempt 的计数器差分得到；attempt 变化或计数回退时重置。p99 显示服务端直方图 bucket 上界（`≤ x`），样本少于 100 显示“样本不足”。
- 诊断包通过 Blob 下载并立即释放 URL。

页面：登录、总览（KPI、健康表、实例、最近审计）、流水线列表（筛选）、流水线详情（概览 / 流量与背压 / 错误 / Checkpoint 风险 / 诊断）、审计摘要、实例与权限。浅色/深色主题；快捷键 `1-4` 切换页面、`Ctrl+K` 或 `/` 快速跳转、`T` 切换主题、`[` 折叠侧栏、标签页左右方向键切换。

## K5.2 资源管理、SQL 编辑与草稿发布

Catalog 迁移 **v4 → v5**（单事务、只新增表，不改历史 pipeline JSON；旧二进制拒绝 v5 catalog；回退靠完整冷备份）：
`authoring_drafts`、`authoring_connections`、`publication_receipts`。

| 接口 | 角色 | 合同 |
| --- | --- | --- |
| `GET /v1/drafts`、`GET/PUT/DELETE /v1/drafts/{id}` | operator | ETag `draft-N`；PUT 无 `If-Match` 只能新建，有则 CAS；DELETE 必须带 `If-Match`。文本可暂时无效。请求上限 512 KiB，解码后文本 ≤64 KiB、metadata（JSON 对象）≤16 KiB；最多 64 份、总计 8 MiB |
| `POST /v1/drafts/{id}/check` | operator | 对草稿当前文本做完整 validate + explain，分别给出结论；不连接端点 |
| `POST /v1/drafts/{id}/publish` | operator | `{operation_id, draft_etag, base_etag}`；重新校验后在**同一事务**里核对回执、草稿 ETag、流水线 ETag 并写入新 revision 与回执；**不启动** |
| `GET /v1/publications/{operation_id}` | 发布者本人 / admin | 持久回执；同 ID+同请求返回原结果（`replayed:true`），同 ID 不同请求 412。保留最近 512 条，更早的 ID 返回 404（不代表没发布过） |
| `GET /v1/pipelines/{name}/revisions[?before=&limit=]`、`/revisions/{rev}` | operator | 有界分页 + 单版本原始配置；同时给出 latest/desired/actual |
| `GET /v1/connections`、`GET/PUT/DELETE /v1/connections/{name}` | operator | 作者侧模板（ETag `conn-N`），按真实 `SourceSpec/SinkSpec` 严格解析；更新不改已发布 pipeline |
| `POST /v1/connections/{name}/test` | operator / handshake: admin | `config` 只校验模板结构；`handshake` 目前对所有类型返回 `probe_unavailable`，不拨号 |
| `GET /v1/secrets` | operator | 只返回名称/存在性，永不返回值 |
| `PUT /v1/streams/{name}` + `If-Match` | operator | 新增条件写入（`absent` = 只新建；否则须等于 `GET` 返回的 `etag`）。**不带 `If-Match` 时保留旧的无条件 upsert 以兼容 CLI/API**；工作台始终携带 |

新错误码：`conflict`（HTTP 412，`current_etag` 在 error.context 中）与 `not_found`（404）。

页面：草稿列表/编辑器（CodeMirror 6 SQL/JSON、Source/Sink 表单 + 保真 JSON、模板套用、Ctrl+S 保存、Ctrl+Enter 检查、冲突横幅保留本地编辑）、发布审阅（当前 vs 目标 diff、Source/Sink/恢复变更、基线是否过期、确认勾选；超时显示“结果待确认”并查询回执，只允许以同一操作 ID 显式重试）、发布后单独“启动”、版本历史（latest/desired/actual + 对比 + 基于旧版本新建草稿）、Stream 字段表单、连接模板与 SecretRef、参考表/插件只读列表。样本查询结果只在页面内存中，闲置 5 分钟清除。

K5.2 未覆盖：网络握手探测（所有类型 `probe_unavailable`）；模板没有历史版本表（只保留当前版本号）；`PUT /v1/pipelines` 的固定 `delivery/recovery` 响应字段未修改（UI 不使用它，以 validate/status 为准）；Graph 模式编辑在 K5.3。

## 测试

- Rust：`cargo test -p sparrow-server --test k5_auth_api`（每个角色对管理路由的直接 API 反例、401/403 不写审计、viewer 响应与审计不含 SQL/路径/token、legacy=admin、`/ui/` 路径与安全头、加载器拒绝符号链接与隐藏文件）；`auth.rs` 单元测试覆盖角色文件 fail-closed 场景。
- 前端：`npm run typecheck && npm test && npm run build`（无损 JSON、API client、轮询、速率/p99/状态推导）。
- 浏览器：`SPARROW_SERVER_BIN=target/debug/sparrow-server npx playwright test`（Chromium；需先 `npm run build` 与 `npx playwright install chromium`）。

## 未覆盖

第二浏览器、移动端、高 DPI 未验证；尚无写操作页面；无 TLS 反向代理实测；角色文件不支持热加载（修改后需重启）。
