# 当前生产化候选：安装、恢复与回退

本页说明当前实现合同，不是目标设备认证或正式发行公告。Linux x86_64、Rust 1.98.0、锁定 Cargo.lock；Row/prepared/fusion，支持线性和显式DAG Preview，恢复按独立profile准入。Arrow/JIT、HA、可靠MQTT均未开启。可选 [持久 HTTP outbox 与输出 DLQ](DURABLE_OUTPUT.md) 提供本地落盘确认（不是远端 2xx）、有界重试和人工处置；启用后必须同时备份 catalog/checkpoint/outbox，且 pipeline stop 不停止积压投递。版本号仍为0.1.0，tag/push另行授权。最新时间图见 [v18/v19 验收](#time-graph-validation)，线性时间组合见 [v16/v17 验收](#linear-time-validation)，单个时间算子历史批次见 [v14/v15 验收](#paused-time-validation)，静态表/迟滞见 [组合证据](#k1-k4-reference-validation)；历史门禁不自动代表新增能力。

新增 K2 **可选 JetStream Preview**：`SPARROW_JETSTREAM=1` 仅为 Server 启用 SDK，默认构建及 HTTP CLI 不链接它。合同、v4 与 File/v3 的目录隔离、资源限制和未验证边界见源码 `docs/JETSTREAM.md`（启用 feature 的包内同时提供）。不要将 R11 的 File/MQTT 数据或下面的默认部署合同直接当成 NATS/TLS/WAN/长稳认证。

<a id="live-lookup-validation"></a>
## 动态参考表与 HTTP Lookup：2026-10-05

基线 `1b86706`，服务器 `box@100.64.0.19`，Linux x86_64 GNU / Rust **1.98.0** / locked Release。证据根 `/workspace/bench-compare/lookup-core-20261005-mwdbLw`。新增范围是管理面CAS增量发布/回退、显式`follow_latest`和HTTP异步查询，不是数据库CDC位置恢复或任意外部查询的aligned认证；合同见 [REFERENCE_TABLES](REFERENCE_TABLES.md)。未在本机运行Cargo/Go/C编译，也未恢复JetStream高负载专题。

- 默认**16成员**＋JetStream feature：`full-r5.log/.exit=0`，**1061 passed / 30 ignored**。不是包含两个实验包的全workspace，也不把ignored计入通过。
- 精确Cargo JSON artifact路径冻结到`frozen-final-tests/`，`new-tests-final.list`为**53项**：Catalog13、Control集成/门禁4、Runtime19、HTTP transport14、Server API2、CLI1。每项**5轮，共265 passed / 0 ignored**，`repeat-final-*.log/.exit=0`。覆盖真实SQLite COMMIT BUSY后的回滚/同CAS重试、head倒退、schema/key/owner/恢复拒绝、并发顺序/缓存/超时/取消、cold future与逃逸诊断信用、HTTP连接复用/严格协议/坏TLS/环境proxy隔离，以及实际DAG刷新故障指标不漏记、不重复合并connector计数。
- 独立no-default-feature **Control＋Server：162 passed / 5 ignored**，`no-demo.log/.exit=0`。这5个既有外部插件fixture未在本批显式重跑，不借用前批的执行结果充当本批结果。
- 实际no-demo候选包`production-package/`的六个binary及依赖/feature隔离检查通过，`production.log/.exit=0`；`package-verify.log`校验通过。`SHA256SUMS`自身SHA-256为`8543b90571019828b54c0ac14b357887f0f69d82e53f43597956cf3ad92df867`。包标识为基线commit加源码指纹的candidate，不冒充正式release/tag。
- 同一生产包、独立Go标准库fixture及修正后的`lookup-process-runner-r2.sh`，**3个全新catalog/进程基线**：`process-r2/r3/r4`全部通过，每轮**20行独立golden、12次Lookup请求、峰值在途2**。真实API/CLI验证增删/CAS412/新revision回退、静态pin不变、热跟随实际revision/hash、schema故障hold/兼容发布不自动复活/显式start与Server重启门禁；SQL/Graph的hit/miss/NULL key、正负缓存/TTL、503降NULL且不缓存、协议/超限硬失败、并发顺序和停止取消均通过。每轮末尾在服务尚运行时确认五项`process_credits`均为0，fixture inflight为0；进程TERM及wait正常，不以进程退出替代Job信用回收。
- 首个`process`仅因测试脚本把API的`error.context`数组误当对象而停止，API实际已返回正确412及expected/current revision；修脚本为`from_entries`，并补全末尾信用oracle，不重编Rust。原失败目录、`lookup-process-runner-r1.sh`保留；修正runner独立归档，不把生产构建当时的源码指纹改写为包含后补harness。
- 最终**510个代码/构建/测试文件**与服务器`source/`一致，`source-delivery.sha256`自身SHA-256为`82748d69b3c15e4fb465f443844ba545c286195ea4469a78074ab07f3e0fb9b8`，`delivery-verify.exit=0`。构建时源码另存`source-built/`并对生产包源码清单复核，`build-source-verify.exit=0`；构建时清单`source-final.sha256`摘要为`9df95544943fc6cf7bcdf75e88f7bb71f6cd8892e546041ce346e901ab352aa0`，与最终交付仅runner不同，Rust内容不变。编排`lookup-validation.sh`/`lookup-validation-r5.sh`、定向复验、每轮进程日志和退出码均保留；正式文档的后补验收说明不修改已冻结包。
- 保留所有失败：R1/R2是扩展JSON宏达到递归上限，拆分对象构造修复，没有提高全局递归限额；R3的1060项通过后补了DAG根诊断合并反例。R4出现一次既有Hysteresis v12测试的`checkpoint wait timed out`；不改代码、不放宽期限，同一编译产物定向**5轮**及原参数完整R5重跑通过，日志为`hysteresis-recheck-*`。**该偶发超时根因尚未确认**，发行验收需进一步跟踪，不能将成功重跑当作根因已经修复。

这是限定Preview的功能/故障验证，不是24/72h soak、WAN容量/p99、aarch64或完整生产放行。保守scratch、同key请求合并、缓存查找与刷新成本另列 [OPT-019](OPTIMIZATION_BACKLOG.md#opt-019)；不以增大预算、弱化取消或使用今天的数据替代历史依赖来“修复”性能。

<a id="external-plugins-validation"></a>
## 外部 SDK 与插件核心整体验收：2026-09-29

基线 `e87b50c`，服务器 `box@100.64.0.19`，Linux x86_64 GNU，按仓库锁定的 **Rust 1.98.0** 构建（服务器默认1.98.1未用于本批）。证据根 `/workspace/bench-compare/plugins-core-20260929-qkkVtm`，本批源码 `source-extension/`。本机没有Cargo/Go/C编译，也没有恢复JetStream高负载专题。

- 默认**16成员**、locked Release＋JetStream feature：`extensions-full-final.log/.exit=0`，**1008 passed / 30 ignored**。其中新增8个外部fixture测试在下述独立测试中全部显式执行，不把ignored当通过。首次全量 `extensions-full-r1` 也是1008通过，当时仅6个新fixture测试；后续补足停止背压和资源不足前不Open的反例。
- no-demo **Control＋Server**：`extensions-no-demo.log/.exit=0`，**146 passed / 5 ignored**；这5个Control外部fixture另行重复执行。此口径比历史仅Server/CLI的48项更宽，不混写为同一组统计。
- 独立 `examples/extensions` 项目只依赖SDK/serde/libc，Source、Sink、Transform共用示例程序但每角色独立批准；另建conformance故障程序。`extensions-example-build.log/.exit=0`。Host 3项＋Control 5项＋SDK 3项，各**5轮，共55 passed**，`extensions-{host,control,sdk}-repeat-1..5.log/.exit=0`。覆盖schema/NULL/精确整数、frame/展开/sequence/水位拒绝、崩溃/OOM/超时/取消、8进程名额、FD/环境清理、pin、Source确认与Sink flush、线性/DAG、单格背压队列、停止回收、配置数据不误当表达式、历史引用/退休、重启及失败不自动重放、预算不足不创建Sink文件。
- 冻结标量及包回归：native **7**、JS **17**、WASM **5**、packages **4**，`extensions-{native_registry,scripts,wasm,packages}-regression.log/.exit=0`。原生format1 v1 manifest identity与前批 `packages-process-native` 完全相同。测试可执行文件由Cargo JSON精确路径冻结在 `extensions-frozen-tests/`，有 `extensions-test-artifacts.tsv` 和SHA256清单，不按残留target文件名猜版本。
- 真实no-demo Server/CLI：`extensions-process.log/.exit=0`，强制Ed25519、默认关闭、Source→Transform→Sink、manifest v1→v2→v1的配置输出 **42→63→42**、checked overflow不发布部分结果、失败不自动重试、持久引用、safe-mode、重启和停用/卸载通过。外部示例的两个revision使用相同executable、不同manifest/config；这是版本绑定/回退验证，不是不同代码版本或性能比较。
- **混合后端** `extensions-mixed-process.log/.exit=0`：外部Source→JS double→WASM double→原生double→外部Transform→外部Sink；**10轮，每轮2048行，共20480行，全部按序得到输入×16**。含管理/启动/停止的总时长 **29s**，不能用它推导容量。每轮结束external session=0、标量worker=2；停用后两种worker均为0。服务RSS第1轮 **19508KiB**、第10轮 **20708KiB**（+1200KiB）；保留逐轮 `/proc` 和子进程快照，不把两点RSS或29s重复试验称为24h长稳。
- 三种标量真实升级回退/有限查询资格/重启、包签名/依赖/撤销/退休、JS独立alarm边界均再次通过：`extensions-{native,script,wasm,packages}-process.log/.exit=0`、`extensions-js-standalone.log/.exit=0`。
- **实际生产候选包** `extensions-production-package/`：独立no-default-feature构建6个binary，发行依赖/feature检查通过，SDK/示例/锁文件/正式文档在包内；`extensions-production.log/.exit=0`。从包内源码再次独立构建样例，`extensions-shipped-sdk.log/.exit=0`；SDK依赖树确认不链接runtime/model/control/server/plugin/connectors；包校验和验证通过。`SHA256SUMS`自身SHA-256为 `e2a2181a0ca90b3053f35d4364ccb926e4e3e23ca3bf5aef196b5c21030c773b`。包是以基线commit标识、源码指纹区分的candidate，不冒充已打tag的正式release。
- 最终**499个代码/构建/SDK/测试文件**与服务器一致：`extensions-source.sha256`自身SHA-256为 `a62b9b7b8390a56b38e268156d7f6beb7a068063b26585f089f0ba756349e563`，`extensions-source-verify.exit=0`；完整复现编排在 `extensions-validation.sh`。最终验收注记是后补证据索引，不据此重新标记已冻结包的文件hash。

自查收紧了外部进程错误后的Flush/Close行为、Source批次与Kernel上限、配置容器计费、失败/重启显式启动门禁及Sink错误不得记为completed。当前原生/JS/WASM函数、包管理及独立Transform/Source/Sink SDK的**约定插件核心范围已齐备**。仍是fresh-only Preview，不是任意第三方插件、aarch64、24h soak、完整存储故障、多租户OS沙箱或任意aligned/exactly-once的认证；性能/批量IPC与长稳按 [OPT-017/018](OPTIMIZATION_BACKLOG.md) 后续处理，支持边界与回退见 [EXTENSIONS](EXTENSIONS.md)。

<a id="package-lifecycle-validation"></a>
## 包签名、依赖及持久引用：2026-09-29

基线 `26fc3a3`，`box@100.64.0.19`，同证据根 `/workspace/bench-compare/plugins-core-20260929-qkkVtm`，本批为 `source-package/`，不覆盖前批 `source/`。交付离线Ed25519工具及发布者范围/撤销策略、format2精确部署依赖图、ELF平台依赖检查、显式attest、catalog v4历史引用保护和带ETag的停止态fresh规则退休。仍须管理员明确批准hash，不把签名当安全沙箱或隐式下载/动态链接器。

- 默认15成员 locked Release＋JetStream feature：**1005 passed / 22 ignored**，`packages-full-final.log/.exit=0`；no-demo Server/CLI **48 passed / 0 ignored**，`packages-no-demo.log/.exit=0`。第一次全量仅两个旧迁移测试把成功版本硬编码为3，改为`CATALOG_SCHEMA_VERSION`；保留`packages-full-r1`，R2/R3及最终均通过。自查补强策略/FIFO读取和保护表的PK/FK结构验证。
- 新包签名/依赖4项×5＝**20 passed**；冻结原生7、JS17、WASM5项回归通过，`packages-repeat-*`、`packages-{native_registry,scripts,wasm}-regression`退出0。全量同时覆盖旧目录v3→v4回填、失败事务回滚、历史引用不随最新SQL消失、退休CAS/停止态门禁、缺保护表拒绝及SQL AST排除注释/字符串误识别。
- 六个生产工具/入口构建成功，`packages-build.log/.exit=0`，`packages-frozen/`与`packages-frozen.sha256`固定匹配产物；测试路径从最终Cargo artifact消息取出，不猜target文件名。没有本机编译。
- `packages-process.log/.exit=0`与`packages-process/result.json`：真实签名工具→认证API/CLI；拒绝unsigned/签名重放/未启用依赖，父包阻止依赖停用，查询输出42；停用后仍因历史引用拒绝卸载，明确退休后释放；旧身份attest前后hash相同，强制签名重启通过。撤销后正常启动给出`policy_denied: plugin publisher signature ...`，safe-mode可启动且worker数0，未伪装热撤销已有进程。
- 三后端实际Server/CLI升级回退、safe-mode、重启、持久引用拒绝/退休及热卸载/原生驻留拒绝均通过：`packages-process-{native,script,wasm}.log/.exit=0`。原生v1 manifest identity与前批相同。
- **483个代码/构建/SDK/测试文件**匹配，`packages-source.sha256`自身SHA-256为 `afd935edcd5dbd0637f69995633cdaf1774e53e5c85b7a97e2f6408bd4390fbe`，`packages-source-verify.exit=0`。证据脚本 `sparrow-packages-validation.sh`。

catalog v4不可由旧v3二进制直接打开；回退要恢复匹配catalog、插件目录和信任策略。退休仅处理全部历史为停止态、无checkpoint的fresh规则，不删除外部文件，不泛化为checkpoint GC。尚未宣称任意OS/动态库闭包安全、磁盘故障矩阵、完整发行包验收或全插件阶段完成；下一子批为外部Transform/Source/Sink及其隔离SDK。

<a id="wasm-plugins-validation"></a>
## WASM 标量：2026-09-29 限定 Preview

基线 `c2219a7`，服务器 `box@100.64.0.19`，证据根 `/workspace/bench-compare/plugins-core-20260929-qkkVtm`，本批源码 `source/`。Wasmi 2.0.0只进入独立worker，配套离线WAT构建工具、C-layout wasm32头文件及真实可安装样例；WASM与原生/JS共享显式hash批准和SQL/Graph绑定，不开启任意恢复资格。

- 默认15成员、locked Release＋JetStream feature：**997 passed / 22 ignored**，`wasm-full-r3.log/.exit=0`；no-demo Server/CLI **48 passed / 0 ignored**，`wasm-no-demo.log/.exit=0`。没有本机编译，没有恢复JetStream高负载专题。
- Server/CLI/JS worker/WASM worker/离线pack构建通过，`wasm-build.log/.exit=0`；最终可执行文件冻结在 `wasm-frozen/`，`wasm-frozen.sha256`。通过Cargo当前feature profile的 `wasm-test-artifacts.jsonl`冻结测试，避免按同名旧target产物猜版本。
- WASM 5项×5轮 **25 passed**，`wasm-repeat-1..5.log/.exit=0`；冻结JS17项回归通过，`wasm-js-regression.log/.exit=0`。覆盖7种scalar/精确整数与NULL、64KiB payload、非法输出指针/UTF8/长度/类型、无限循环fuel、拒绝memory增长/初始超限、OOB/trap、ABI/导出/import/start拒绝、500次新实例全局重置、SQL/Graph、有限查询跨行预算、取消、safe-mode/重启/pin/热卸载以及恢复资格拒绝。
- 真实Server/CLI三条后端均通过：`wasm-process-{wasm,script,native}.log/.exit=0`。WASM v1→v2→v1输出42→63→42，有限查询输出42及预算拒绝、模块hash/输入bytes可观测，safe-mode/重启/运行pin及热卸载验证；原生仍拒绝有限查询、仍需重启才能卸载驻留代码。WASM样例由发行工具从WAT构建，不以测试内伪返回代替插件进程。
- 首轮 `wasm-full-r1`是feature API编译失败：memory64未编入时不存在关闭该proposal的方法，移除该调用并保持feature关闭。第二轮 `wasm-full-r2`只有新增内存测试失败：把拒绝 `memory.grow` 只能表现为trap的预期写错，现按WASM的`-1`语义验证拒绝增长，宿主上限不放宽。另修正预存的effective插件视图将所有后端都写成native/non-preemptible的问题。原始日志保留。
- 初始冻结脚本因target中多份JS测试同名而退出，未运行进程测试；改由Cargo返回准确artifact路径后完成上述验证，没有重编造试次。最终 **476个代码/构建/SDK/测试文件**本地与服务器一致，`wasm-source.sha256`自身SHA-256为 `ed45d7cb2d464e181a5b5323f3e4c7c1d7619e022c6d437d238631ee7259df8a`。

剩余签名/包依赖/持久引用、外部Transform/Source/Sink仍是后续子批。**未宣称**WASM业务吞吐/p99、长期RSS/soak、aarch64、完整OS多租户沙箱、任意插件aligned或整个插件阶段完成；WASM fuel不是整次查询的精确工作计量。

<a id="script-completion-validation"></a>
## JavaScript 收尾：缓存、诊断和有限查询

2026-09-29，基线 `96dab24`，服务器 `box@100.64.0.19`，证据根 `/workspace/bench-compare/scripts-completion-20260929-Yu0lr7`。本批只收尾 JS 标量，不代表 WASM、签名/依赖协议或 Source/Sink 插件 SDK 完成。具体边界见 [PLUGINS](PLUGINS.md#有界诊断与有限查询)。

- 代码/测试集中完成后在服务器验证；没有本机 Cargo 编译。首轮991/48及专项全部通过。随后自查发现QuickJS列号是UTF-8字节而非UTF-16，补坐标转换及CR/LF/CRLF/U+2028/U+2029换行测试，并对含中文/emoji的真实脚本验证精确位置；原首轮源码/日志/冻结保留在 `source-r1/`、`*-r1.log`、`frozen/`。没有隐去初轮或以其替代最终结果。
- 最终默认14成员 locked Release＋JetStream feature：**992 passed / 22 ignored**，`full-r2.log/.exit=0`；不是包含实验包的全 workspace。新增 ignored 是显式性能微测试，随后独立运行通过，不把其余 ignored 算作通过。
- no-demo Server/CLI：**48 passed / 0 ignored**，`no-demo-final.log/.exit=0`；生产入口构建通过，`build-final.log/.exit=0`，Server/CLI与首轮逐字节相同，只有worker代码/测试改变。匹配的普通 worker、Server、CLI、17项集成测试和worker单元测试冻结在 `frozen-final/`，指纹 `frozen-final.sha256`，来源 `frozen-final-origins.txt`。
- 同一最终冻结 worker：17项脚本集成×5轮＝**85 passed**；4项非性能worker单测×5轮＝**20 passed**，`scripts-final-1..5.log/.exit` 与 `worker-final-1..5.log/.exit` 全为0。覆盖缓存hash/上限与重新启用、严格模式/Function.toString/全局/原型/闭包隔离、compile/initialize/call诊断及Unicode坐标、恶意getter/Proxy不触发、总预算跨行/嵌套调用/NULL/失败不退款、SQL与Graph有限查询、超时/调用方取消与pin/输出内存回收，同时重跑原有类型、资源、并发和生命周期反例。
- `process-script-final.log/.exit=0`：真实认证API/CLI查询输出42、整次预算拒绝、错误仅含阶段和数值坐标（实际 `call`/`1:23`，无 `PRIVATE_PAYLOAD`），缓存身份和有界大小可观测；File→JS→File升级/回退42→63→42，safe-mode、重启、故障隔离、pin、热卸载全部通过。
- `process-native-final.log/.exit=0`：原生仍拒绝有限查询，驻留后仍须停用＋重启才能卸载；v1 manifest identity 与前批 `scripts-20260929-MKWwFR/native-process-final/install-v1.json`相同。`worker-limits-final.log/.exit=0`：独立worker复杂正则受控失败、超大frame/截断header拒绝；退出0，不称为实际触发SIGALRM的验证。
- **缓存微测试** `cache-bench-final.log/.exit=0`：同一测试二进制、新VM/相同bootstrap/64MiB堆和中断期限，解析与缓存按ABBA顺序；每轮每项2000次＋20次预热，两项共16000次。各模式两轮p50均值：BigInt **165.4→116.0μs**，JSON **175.5→119.0μs**，下降约30～32%。这是无IPC的进程内libtest微测，没有worker进程RLIMIT，**不与上一批Boa选型数值直接比较，也不代表流水线吞吐/p99或生产容量**。

本地与 `source/` **466个代码/构建/SDK/测试文件**逐一匹配，`source-final.sha256`自身SHA-256为 `7ce0144126164b1c58476e3fe264009dfe0e47a4c81a75ba5a8bbc97a1df6e45`；`source-verification.exit=0`。复现命令见证据根 `sparrow-js-final-validation.sh`，正式文档另行归档，不改变已测源码。自查覆盖原生hash不变、仅内部生成bytecode、C值/缓冲区所有权、无用户getter诊断、task-local预算隔离及失败回收。

内部IPC升级ipc2，Server/worker必须配套部署；JS manifest target/源文件身份不变。仍为限定Preview：**未做aarch64实机、长时间soak/RSS、多规则公平性、完整发行打包、进程强杀/OS沙箱矩阵、aligned脚本恢复或全系统生产认证**。JetStream 10k/20k专题继续暂停。

<a id="script-plugins-validation"></a>
## JavaScript 标量：2026-09-29 限定 Preview

基线为原生标量提交 `09db48b`。同一服务器 `box@100.64.0.19` 的证据根为 `/workspace/bench-compare/scripts-20260929-MKWwFR`，最终选型为 **QuickJS-ng 0.16.2 / rquickjs 0.14.0 + 独立执行进程**，不保留生产 Boa 依赖。具体支持边界见 [PLUGINS](PLUGINS.md#javascript-标量函数)。没有恢复 JetStream 高负载专题，没有本机 Cargo 编译。

- 选型微测试：`engine-evaluation-r4.json`、`engine-results-r4.jsonl`、`engine-summary-r4.json`、`engine-source-r4.sha256`、`engine-evidence-r4.sha256` 与 `frozen/engine-eval-r4`。Rust 1.98.0，Linux x86_64/8 vCPU；ABBA、6 项×1,000次×4轮（24,000次），每项另预热20次；共享进程隔离策略而非混比同进程与跨进程。QuickJS 的 64MiB 内层堆限制及中断回调开启，128MiB 进程地址空间与100ms父进程期限两者相同。
- 两者各测无限循环、嵌套循环、过大 ArrayBuffer、递归、复杂正则、超大 BigInt。正常用例全部完成，异常由引擎拒绝或父进程超时终止；这不是任意恶意代码的安全证明。`engine-run-r4.exit=0`。首次 Boa 10k 循环预算拒绝64KiB JSON，调整1M后重跑；原始日志及 R3 源码归档保留，未拿配置失败算作性能优势。
- Boa 原型仅作为选型验证：`scripts-tests-r3.log` 的10项脚本＋7项原生通过，不冒充最终 QuickJS 验证。其初轮被 Cargo 产物 group-write 权限拒绝，随后测试按安全安装方式复制私有 executable；第二轮暴露统一1.5MiB scratch超过默认Job配额，改成按声明类型有界预留并收紧响应frame，未放宽配额。

最终 QuickJS 源码目录 `source-quickjs/` 与本地 **464 个代码/构建/SDK/测试文件**逐一匹配；`source-final.sha256` 自身 SHA-256：`8f10e34e59cc4bc23265f2cfda07c222726d9cfcd9206c5ca97705ad85ff6e47`。文档收尾另记，不改变 Rust 验收构建。最终二进制和脚本测试可执行文件位于 `frozen/final/`，指纹为 `frozen-final.sha256`。

- 默认14成员、locked Release、`--features sparrow-server/jetstream`：**982 passed / 21 ignored**，`quickjs-final-full.log/.exit=0`。不是包含实验包的 `--workspace`，ignored不算通过。
- 独立 no-demo Server/CLI：**47 passed / 0 ignored**，`quickjs-final-no-demo.log/.exit=0`；Server/CLI/worker 的生产二进制构建通过，`quickjs-final-build.log/.exit=0`。Server/CLI 的 normal/build 依赖图没有 JS 引擎，worker没有意外开启 allocator/rust-alloc/loader，见三个 `*-dependencies.txt` 及 `worker-features.txt`。打包/冻结脚本做了语法与依赖门禁检查，未冒充完整发行包验收。
- 最终冻结 worker＋12项脚本测试，重复5轮：**60 passed / 0 ignored**，`script-test-list.txt`、`scripts-repeat-final.log/.exit=0`。覆盖7类scalar/精确64位BigInt/NULL、UTF8与Bytes边界、动态构造器拒绝、异步结果拒绝、64MiB VM内存上限、无限循环/递归/超时、取消/Job退款、全局状态隔离、版本/pin/重启/热卸载、worker配额与失败恢复、SQL/Graph以及fresh-only拒绝。
- 真实 no-demo Server/CLI：`process-final.log/.exit=0`、`process-final/result.json`，File→JS→File，v1/v2/回退输出 **42→63→42**；另让复杂正则使Job失败，Server保持健康，其他版本继续输出42。运行pin、safe-mode、正常重启恢复批准版本、失败worker拒绝伪重新启用、停用后热卸载及缺包拒绝均通过。复现：`scripts/plugins-script-smoke.sh SERVER CLI NEW_DIR`，worker放在Server同目录或显式设置 `SPARROW_JS_WORKER`。
- 原生真实进程回归 `native-process-final.log/.exit=0` 通过，仍要求驻留版本停用后重启才能卸载。v1 manifest identity 与09-27历史 `process-reviewed/install-v1.json`完全相同，未改原生manifest编码/hash合同。
- 无Server父进程期限控制的worker测试：`worker-limits-final-r2.log/.exit=0` 与目录内 `result.json`，复杂正则返回受控Error，超大frame/截断header被拒绝。本次 `standalone_exit=0`，**没有把它当成实际触发SIGALRM的证据**。首轮缺少测试脚本依赖 `xxd`，在启动worker前退出；改成Bash帧头编码后通过，未改引擎限额或Rust代码。复现：`scripts/plugins-worker-limits-smoke.sh WORKER NEW_DIR`。

初轮 QuickJS 982/47与真实脚本进程试跑也通过。本轮review覆盖声明相关IPC预算/错误上下文、继承信号清理、无脚本Job不安装task-local及独立worker冻结；最终Rust候选重新通过上述完整回归。验收脚本去除xxd依赖、打包脚本扩大SDK/示例指纹范围属于收尾Shell改动，另做语法/依赖与真实worker检查，未因此再编译Rust。过程源码/失败日志仍保留，源码指纹不以“同分支/同版本号”替代。

**首批当时未宣称**：长时间 soak、aarch64实机、父进程SIGKILL/独立SIGALRM/掉电故障矩阵、完整发行包认证、seccomp/namespace安全沙箱、多租户隔离、流水线容量认证、aligned恢复或有限查询脚本资格。缓存/安全源码栈/有限查询已在后续[JS收尾](#script-completion-validation)交付；批量IPC/实例池与容量继续归 [OPT-017](OPTIMIZATION_BACKLOG.md#opt-017)，第8批整体仍未完成。

<a id="native-plugins-validation"></a>
## 插件共同管理与可信原生函数：2026-09-27 首个子批

验证基线 `c413707`（已提交的有界分析）；本段证据对应其后的原生函数工作区。服务器 `box@100.64.0.19`，根 `/workspace/bench-compare/plugins-20260927-29sBzf`，最终源码 `source`，no-demo二进制 `frozen/`。457个代码/构建/SDK/测试文件的本地与服务器指纹一致，`source-commit.sha256` 自身SHA-256为 `246a759225bbca3597332f60bd0e42fdae1831ba2ada2ffabc660194b20a08a8`。提交前仅清理新Cargo.toml尾部空行，再次执行全量/专项/no-demo验证；生成的Server/CLI与进程验证二进制逐字节相同，原`source-final.sha256`/reviewed日志仍保留。验收后的文档另存，不改测试源码。Rust/Cargo1.98.0，Linux x86_64；没有本机Cargo编译，也没有恢复JetStream高负载专题。

- 默认13成员（新增sparrow-plugin）、locked Release、`--features sparrow-server/jetstream`：**970 passed / 21 ignored**，`tests-commit.log/.exit=0`。ignored不算通过，也不是实验workspace全部验收。
- 同一最终源码新 `plugins_` 13项×5轮：**65 passed / 0 ignored**，`repeat-commit.log/.exit=0`。每个测试进程加载真实C共享库，使用私有临时目录；不是13种性能档。
- 独立生产入口 `cargo test --locked --release --quiet --no-default-features -p sparrow-server -p sparrow-cli`：**47 passed / 0 ignored**，`no-demo-commit.log/.exit=0`；随后同scope的`cargo build --bins`退出0，`build-commit.log/.exit`。
- `process-reviewed.log/.exit=0`、`process-reviewed/result.json`：使用真实Server/CLI与两个独立artifact，输入21输出v1=42、v2=63、回退v1=42；运行中disable拒绝、safe-mode保留approval但不加载、正常重启自动恢复批准版本、驻留uninstall拒绝、停用后重启允许卸载、旧规则缺包拒绝。只操作独立catalog/目录/子进程，未修改已有服务。`frozen.sha256`记录匹配二进制。
- 首轮完整回归969/47、新12×5通过；review后新增覆盖7种scalar的矩阵测试，总计新13项，并增加加载前审计断言，形成上述最终970/47/65。初轮 `process-final` 被测试输出目录的group-write权限拒绝；`process-r2` 已验证第一段42输出及运行pin，后被缺少If-Match拒绝更新。脚本补`umask 077`与真实ETag，未降低生产校验；`process-r3`及最终reviewed完整流程通过，所有初始失败记录保留。

本批只开放[PLUGINS](PLUGINS.md)所列的可信原生标量合同；版本/hash pin、目录锁、sealed加载、默认关闭、原生边界前审计、NULL/类型/输出检查、元数据/host输出额度及Job结束退款均随本批review/验证。native不提供沙箱、强制中断或热卸载，有限查询拒绝native，aligned/恢复资格不继承旧profile。脚本/WASM、外部Transform/Source/Sink、签名验签及递归动态依赖固定仍未完成。

**NOT RUN**：aarch64实机、长期负载/RSS、ABBA性能、SIGKILL/掉电/磁盘故障矩阵、完整发行包验收、Clippy零告警及整体生产认证。C ABI的输出检查不能保护宿主免受恶意机器码破坏；可信代码前提不可省略。

<a id="analysis-validation"></a>
## 函数与有界分析：2026-09-27 限定 Preview

验证时基线为 `728b228`，本批当时是该提交之后的工作区候选；后续提交不改写验证指纹，无push/tag。服务器 `box@100.64.0.19`，证据根 `/workspace/bench-compare/analysis-20260927-lPMZOQ`，最终源码目录 `source-r4`；目录名不是额外版本/性能承诺。440个代码/构建/测试文件由 `source-final.sha256`逐一核对，本地与服务器一致，清单自身SHA-256为 `270f688f783ffd1512532b9905552b712c65c0f4790b47fe793432042ca2981a`；文档验收更新另存，不改测试源码。

- 默认12成员、JetStream feature、locked Release：**957 passed / 21 ignored**，`tests-final.log/.exit`（0）。未用 `--workspace`混入实验包，未把ignored的真实broker/介质测试算作通过。
- 独立无demo Server/CLI：**45 passed / 0 ignored**，`no-demo-final.log/.exit`（0）。命令为 `cargo test --locked --release --quiet --no-default-features -p sparrow-server -p sparrow-cli`，包括新API集成测试，不仅是lib测试。
- 同一最终源码/feature编译结果，新 `analysis_` 清单20项×5轮：**100 passed / 0 ignored**，`repeat-final.log/.exit`（0）。包括真实双File→Join→required HTTP、认证查询API、独立Kernel/响应准入、取消/额度退款，及独立数学oracle、SQL/Graph/恢复拒绝矩阵；不是20种压力档或长稳。
- 全量命令：`cargo test --locked --release --quiet --no-fail-fast --features sparrow-server/jetstream`；重复命令在同一范围增加过滤 `analysis_`，最后做no-demo入口验证。本机未进行Cargo/Go编译；未重启或修改现有服务。
- 初轮 `tests-r1/r2` 暴露新枚举漏分支与新future尾表达式借用，已修复。首个可执行全量 `tests-r3` 有两个新增Control失败：SQL Join的graph_io被旧“仅Graph authoring”校验拒绝，以及测试把Dynamic UInt64正整数交给旧受限CAST。前者开放SQL/Graph共同绑定后仍严格核验物理端口；后者改用已有有范围检查的`to_int64`，不暗改旧CAST。原始失败日志保留，后续`tests-r4`及最终全部通过。
- 自查另修正Join双侧时间字段的独立传播、`join_time`下游窗口lineage、typed Timestamp UNNEST、ordinal别名冲突、SQL表修饰拒绝及线性UNNEST元数据预留，并补对应功能/边界测试。

限定功能合同见[ANALYSIS](ANALYSIS.md)。新增UNNEST/Join/聚合无恢复codec，不能继承旧profile资格。**NOT RUN**：新Join/展开容量、ABBA性能门禁、独立生产打包/外部CLI进程矩阵、TLS/WAN、24/72小时长稳、全新SIGKILL恢复、Clippy零告警和整体发行认证。JetStream10k/20k专项继续按用户要求暂停；没有用本批通过改写历史高档失败。集合函数和Join保守额度/扫描成本另记[OPT-016](OPTIMIZATION_BACKLOG.md#opt-016)。

<a id="capacity-validation"></a>
## 可靠链路与容量：2026-09-26 s4（高负载仍有边界）

服务器 `box@100.64.0.18`，证据根 `/workspace/bench-compare/capacity-20260926-Jw3FQJ`。最终源码 `source-s4`、冻结 `frozen-s4` / `production-s4`，发布候选 `package-s4-default/jetstream`、独立驱动 `capacity-driver-s4`。测试时本地424个指纹文件与包源码清单逐一相同；基线HEAD `e3bc558` 不是干净提交。未commit/push/tag；本机未编译Cargo/Go。

- 实现及逐案例数值见 [CAPACITY](CAPACITY.md)：旧小型primitive JSON借用编码并保持字节兼容、普通JetStream可选5～250ms idle cap（默认250）、安全额度错误详情、进程tracked credits观测。默认预算/pending/pull、Explicit ACK、持久性和既有恢复协议不变。
- 最终默认成员/JetStream Release **918 passed / 21 ignored**；独立no-demo **44 passed / 0 ignored**。新5项精确清单**20轮=100次**通过。额外启用 `serde_json/preserve_order` 的两个新测试逐名执行通过，保留宿主feature统一时的旧字节顺序。普通Clippy退出0、119条warning，非`-D warnings`通过；实验Arrow/JIT不混入本批。
- `matrix-s4` 预先声明58试次，**54个有限正确性通过、4个20k失败，exit=1**；新旧各两次20k都触发fetch reply期限。1k/5k、两Job各5k、20ms响应延迟下1k以及HTTP Push500达到各自有限试次目标；10k零状态/Count的p99约8.2～8.8秒，不是持续容量通过。过载恢复能排空但未达到预声明p99。成功及预期拒绝的28个候选试次停止后tracked余额/handle全部为0，HTTP每Job均复用1条输出连接。
- idle250与idle20的12条试次，p99约256～257ms对28ms，pull从101增至364～365。8KiB padding在指定schema下成功；16KiB准确命中decode reservation拒绝，0输出、broker ACK floor=0，API保留安全used/request/cap。小样本、固定schema不是普遍容量保证。
- 原三组ABBA单组全部通过（exit均0），全样本合并也通过；发行验收仍独立判断。
- `legacy-s4/remaining.exit=0`：两包production smoke（各20次生命周期、恢复/备份/回退）；Action27项及两包6场景/2次SIGKILL；Resample24项及9场景/30次SIGKILL；Silence33项及6场景/30次SIGKILL；Alarm14项及6线性/4图场景；paused27普通+1真实broker；两包MQTT live各9项及broker停启、server强杀、75s持续来包通过。两包持续期均3699条、0持续期重连、0错误静默；不是24/72h长稳。
- `extra-s4d.exit=0`：冻结K2专项**36项含真实broker、0 ignored**及旧独立K2进程oracle通过；另跑6个paused-time进程场景和两组100行串行提交成本观察。MQTT1k/5k/10k/20k对照共**32试次**全部有限正确，候选20k实际约19985～19992输入/s、p99约5.03～5.07ms；QoS0不是可靠交付，p99未普遍优于旧Action版本。完整口径见CAPACITY，未把这组有限测试叫长稳或新的eKuiper对比。

首次feature-on smoke在任何server/log/config产生之前退出，空目录与`followup-s4c.exit=1`保留。没有记录到精确失败分支，不能把“端口冲突”写成已证根因；随后在新的证据目录显式选取checked-free端口、启用`bash -x`，完整smoke通过。同一冻结包未重编译，原三组性能结果不重跑、不替换。这项夹具失败不隐去，亦不把未运行到的数据面当作原试次已通过。

### 原File性能门禁

仍为原time-graph-v13 fresh基线、冻结K1驱动、32000/131072行、三组ABBA；周期比较同一s4候选的checkpoint关/开（6400行、100ms、20ms响应等待）。阈值保持fresh≥0.97、periodic≥0.90、RSS增量≤2048KiB，没有增加输入量或选取通过的重跑。全部warmup/测量输出完整、无丢重/非法行，对应hash一致，周期提交成功数为正且失败0。

| 负载 | 合并吞吐比 | RSS增量KiB | 测量数 |
|---|---:|---:|---:|
| fresh / 0 state | 1.025918 | 568 | 36 |
| fresh / 2 states | 0.986700 | 840 | 36 |
| periodic / 0 state | 1.004748 | 452 | 24 |
| periodic / 2 states | 1.001255 | 184 | 24 |

`performance-s4.exit=0`，只说明匹配s4候选通过该原File回归门槛；双Count不是“明确性能领先”，JetStream高档容量也未因此通过。MQTT live/Action旧候选的失败记录继续保留，不用本结果回写历史通过状态。

### 诊断与复验边界

旧Action第二组失败样本的首条HTTP到达中位数为5.44/5.69ms，首末输出跨度67.01/68.72ms；不把总差异全部算成启动或环境噪声。独立1048576行CPU采样显示JSON标量转换和allocator为实际热点之一，支持减少重复JSON对象/字符串分配的改动，**不证明它解释了所有历史回归**。诊断大样本不替代原32000/131072行门禁。

自查修正了两处冷路径遗漏：effective曾仍固定报告250；安全quota只放context而Supervisor只持久化message，导致最终状态看不到详情。最终s4同时补齐真实状态断言。驱动还分离最终ACK/stop计时，修正输入等价吞吐命名、小样本nearest-rank、来源/Job标识及错误时子进程回收。s2旧矩阵/失败保持原样；s3被中止并复用编译缓存，没有冒充最终候选。

生产包已构建成功后，额外preserve_order检查先因离线缓存缺少锁定的indexmap失败；取回依赖后3个名称含capacity的测试全部通过。首版包装器误以为该宽过滤器只应选中2项，随后改为逐名核对两个新增测试，未放宽断言或改变Rust代码。原错误日志保留。最终README/验收文档晚于包内文档；另仅为下一次打包在 `production-build.sh` 增加复制CAPACITY文档的一行（已做shell检查）。该脚本行不在上述包的旧源清单中，Rust/Go/测试输入仍逐文件匹配，不为文档再次编译或改写原包指纹。以本节及原始产物为准。

```text
default server  f1bf6ade64f634319093fbe68de4bd0ddb905a428ae1cc5437418896f086a1d9
JetStream       bdb07d2bcc3d6e6d6e92a7dfb28b42660533852c6595c0e6ba90cb286831436c
process driver  ffcfcfcefc099803721cc14d96213c4f9a61b792df0ce1363272f6c13eaf20b5
source manifest fb5c4b50daed540948d14aea6107a36d6c5101a191f936e423042a98c6ad000d
```

10k/20k可靠持续接入、真实TLS/WAN/目标设备、24/72h及介质故障仍不能宣称通过；第5批剩余与第6批生产门禁保持未完成，前端继续后置。

<a id="actions-validation"></a>
## Action/File/纯函数：2026-09-26（功能通过，性能仍 HOLD）

服务器 `box@100.64.0.18`，本批根目录 `/workspace/bench-compare/actions-20260926-ImXbuk`。最终 Rust 为 `source-s6`，冻结 `frozen-s6`（Release/JetStream）及 `production-s6`（独立无demo）；包为 `package-s6-default/jetstream`，Go driver 为 `process-s5`（s5/s6对应Go源码逐文件一致）。本地逐文件校验包源清单通过。基线标记 `e3bc558` 不是干净HEAD：包括此前未提交的Resample/MQTT live及本批Action；没有commit/push/tag。本机只编辑、格式化和静态检查，Cargo/Go编译均在服务器，重复试验只复用冻结二进制。

- [合同与模板](ACTIONS.md)：HTTP/MQTT/Log字段映射/typed JSON模板、受控HTTP query/单行value及MQTT topic、有界Linux NDJSON File Sink、18个新增纯函数；多动作复用DAG。仅restart_fresh，不开放aligned、可靠MQTT、持久outbox或跨Sink事务。
- 全量Release：**913 passed / 21 ignored**；独立no-demo：**44 passed / 0 ignored**。ignored不是通过，专项broker结果单列。Window future仍为legacy **3272 B** / ordered **3080 B**，原≤3328 B及StreamControl≤24 B门槛通过。
- `validation-s6/exit=0`：固定清单**27×20=540**次精确测试；包括NULL/UInt64/Unicode、严格JSON/时间边界、SQL/Graph共享表达式与融合对照、预算退款、HTTP重试/取消/目标编码、文件轮转/锁/目录替换/坏尾/配额，以及真实Supervisor required/optional多输出失败。
- 独立Go oracle：两包各3类进程场景，合计**6场景、2次真实SIGKILL**。File新尝试重读产生重复但只写新段、不覆盖旧段，坏尾保留、quota失败不删已写数据；HTTP固定path/query正确转义、503重试payload/URL相同且复用连接；真实隔离Mosquitto上的动态topic和payload匹配黄金值。File强杀点依据完整行写计数，不把该计数当作最终fsync/可靠ACK证明，也不是断电/写中撕裂测试。
- `legacy-s6/exit=0`：两包production smoke；Resample24项+9进程场景/30次SIGKILL；Silence33项+6场景/30次SIGKILL；Alarm14项+6线性/4图场景；paused27普通+1broker；两包MQTT live各9项及broker停启、server强杀、75s持续来包全部通过。持续期分别3702/3703条，0持续期重连、0错误静默。未因此声明整个历史矩阵中本次未重跑的独立故障门禁已重新认证。
- 自查修正：极小f64转字符串需要计入长十进制文本及Arc/Scalar开销；新File参数拒绝无意义的网络/QoS选项；MQTT fallback topic在复制前限长；逐行HTTP预算拒绝补齐计数。保留s2内存估算/目录fixture失败、s3测试helper编译失败、s4预算fixture失败；s1误含实验workspace的离线依赖失败也未删除。s5为通过预验证的中间候选，最终证据仅指s6。
- dev/all-targets/JetStream普通Clippy退出0，去重119条风格/简化提示；不是`-D warnings`通过，见OPT-009。新模板重复扫描、保守JSON scratch和逐行HTTP成本见OPT-014。

### 原性能门禁：未通过，不用合并结果覆盖单组失败

原三组ABBA、原输入量、fresh≥0.97、periodic≥0.90、RSS增量≤2048 KiB均未改；仅候选切到Action s6。fresh仍对照原time-graph-v13基线；periodic按原协议比较当前候选的周期checkpoint关/开，不是旧二进制对照。三个组exit为**0、3、0**。第二组fresh/0-state比值 **0.9695805209 < 0.97**，虽然很接近，也不四舍五入为通过。所有原始及合并输出完整、无丢重/非法行且hash一致，RSS门槛均通过。

| 负载 | 三组合并比值 | RSS增量KiB | 测量数 |
|---|---:|---:|---:|
| fresh / 0 state | 0.996476 | 808 | 36 |
| fresh / 2 states | 0.991946 | 1308 | 36 |
| periodic / 0 state | 1.004323 | 280 | 24 |
| periodic / 2 states | 1.000754 | 836 | 24 |

`performance-s6.exit=3`，不重跑挑选样本、不放宽阈值、不据此归因环境噪声；此前MQTT live的0.912865失败同样保留。此测量仍是旧File路径，不是Action吞吐、WAN/p99或File Sink容量认证。下一批优先分离配置启动/稳态成本并定位，再做多规则/慢下游/容量；TLS/WAN、目标设备、24/72h、真实ENOSPC/介质掉电仍NOT RUN。**首批功能开发完成，整批发行仍HOLD。**

```text
default server  68fcbfef9ec34fc2c13ce850b89043a2333295c5eccb2370a8938afa16a53d9f
JetStream       4de63b6b1d2417f33977d68716e557311bd6458098b137861af9e03d0b18c04a
process driver  a69b6a9f7977b8c0f537282b3e4068830ef192def9d4a73eb62c7c541c8873e9
source manifest ce74f3623fb9e0f3b484b53bbc5db59332b9044b36eb928b0e17b1c5aa23343e
```

两个包源清单相同。最终验收说明/README/预算与兼容补充在打包之后更新，不改Rust、Go、测试输入或清单；包内文档是当时版本，以上最终结果以工作区本节及服务器原始产物为准。

<a id="mqtt-live-silence-validation"></a>
## MQTT live 静默：2026-09-26（独立非持久 Preview）

服务器 `box@100.64.0.18`，本批根目录 `/workspace/bench-compare/mqtt-live-20260926-gs7Q5b`。最终 Rust 为 `source-s5`，`source-candidate/crates` 与其逐文件 hash 一致；冻结 Debug `frozen-s5`、Release `frozen-release`，两个包 `package-default/jetstream` 的 source manifest 一致，Go driver 为 `process`。基线 commit 标记仍为 `e3bc558`，**不是干净 HEAD 构建或已发布版本**。本机只编辑/格式化，Cargo/Go 编译均在服务器；重复验证不再编译。

- [实现合同](IOT.md#mqtt-live-silence)：`clock:live`、MQTT QoS0/clean_session、单个 Silence、可选下游纯 Transform、HTTP、restart_fresh。新鲜单 outstanding PINGRESP + 有界 FIFO + sticky epoch + 消费端重新校验；不是 broker 排空/设备故障证明，不复用 File/JetStream journal，不开放 MQTT checkpoint/restore。
- `full-s5`：**885 passed / 21 ignored**；`full-release`：**886 passed / 21 ignored**。Release 的旧 Window future 为 **3272 B**，有序分支 3080 B，原 ≤3328 B 及 control ≤24 B 断言通过；不能把被忽略测试记为成功。
- 正式 production 无 demo 测试配置 `production-frozen`：**44 passed / 0 ignored**。另有 control/server 的 no-default-features 135 项通过（含开发依赖，不拿它替代正式包依赖检查）；两包均通过原构建脚本的依赖隔离与 checksum 检查。
- 新精确清单 **9×20=180**，`repeat-s5/exit=0`；覆盖 FIFO/丢失控制的 sticky epoch、字节/超大行/取消退款、探测与队列新鲜度等号、完整新宽限、能力/恢复拒绝、retained、静默/恢复、连接与管线重启、坏 JSON 两种 policy、无 PINGRESP 但持续 PUBLISH、慢 HTTP/满 ingress 有界取消。冻结可执行文件复用；与全量套件有交集，不累计为独立用例数。
- `mqtt-default` / `mqtt-jetstream`：每包再次跑精确 9 项，再运行隔离的真实 **Mosquitto 2.0.22**。各有一次 broker kill/start、一次真实 Sparrow SIGKILL；核对断连不制造静默、新完整宽限、连接重建不换 generation、进程重启换 generation/清空已观察 key、retained 不登记/恢复、checkpoint API 拒绝。两个 fixture container 均清理，未停止其他 broker。
- 两包各持续约 **75 s** 来包（默认包 **3696** 条、feature 包 **3703** 条），收到/解码完整、无 ingress 丢弃、持续来包阶段重连 0、PINGRESP timeout 0、活跃设备误静默 0。停止来包后产生预期静默。这是约 50/s 的低速保活/正确性试次，不是 10k/20k 容量、WAN/p99、eKuiper 对照或 24/72 h soak。
- 重点旧恢复回归：`legacy-regression/resample`（24 项、两包 9 进程场景/30 次 SIGKILL）、`silence`（33 项、两包 6 场景/30 次 SIGKILL）、`alarm`（14 项、线性 6 + 图 4 场景）均通过；旧 paused 精确 27 regular + 1 broker 用例通过。每个旧清单本轮一次，不冒称重复 20 轮；本批未重新执行所有历史容量/长稳矩阵。
- 普通 all-targets/JetStream Clippy exit0：按主 span/code/message 去重 **113 条诊断**，原始 compiler-message 194 条（含重复目标诊断）。两个新项是新 connector 文件的 test-module 排列与测试 helper 的 needless borrow，记录为样式债务，未加 `allow`，不声称 `-D warnings` 通过。

### 开发失败与证据边界

保留 s1 的 permit 借用错误、s2/s3 的测试 ID 类型错误及 s4 慢 HTTP 断言失败。s4 把“所有仍持有 Queue credit 的行数”错当成“FIFO occupancy”；自查后新 Source 改为一次仅取一个事实，不走旧批量预取，断言明确为容量 2 加队列外 1 个工作事件，s5 的全量/重复/故障验证通过。普通 Source 未改为逐行。

旧 paused runner 首次把复用 fixture 的 `live_silence_tests` 子模块误选入原清单而拒绝（`legacy-regression/exit=1` 原样保留）。只修正发现阶段的模块排除，不变更旧 expected 清单；`runner-final` + `legacy-regression/paused-final/exit=0` 使用相同冻结 Rust。包内 source manifest 是修正该测试脚本前的候选；本地最终文档与 runner 更新不回写冻结包。Go 首次错误地按 module 构建无 go.mod 的 fixture，改用显式 `*.go` 后 build/vet 均通过，不涉及服务端生产代码变化。

### 性能与剩余发行门禁

按预定三组完整 ABBA，仍使用原 File fresh ≥0.97、periodic ≥0.90、RSS 增量 ≤2048 KiB 门槛；before 为 `time-graph-artifacts-20260923/package-v13-default`，after 为本批默认包，periodic 为同候选的 checkpoint on/off。**第二组 fresh/0-state 为 0.912865，未通过；`performance.exit=3`，本轮不能整体放行。** 另外两组该项为 1.033513 / 1.059092。全部正确性、输出 hash 和 RSS 门槛通过，所有样本保留，不拿下方合并值覆盖单组失败。

| 场景 | 三组合并吞吐比 | RSS 增量 KiB | 有效测量试次 | 门禁说明 |
|---|---:|---:|---:|---|
| fresh / 0 state | 1.017236 | 456 | 36 | 合并通过，第二组未过 |
| fresh / 2 states | 1.002886 | 580 | 36 | 单组及合并通过 |
| periodic / 0 state | 1.004979 | 264 | 24 | 单组及合并通过 |
| periodic / 2 states | 0.997656 | 232 | 24 | 单组及合并通过 |

第二组 before/after 都出现变慢；原零状态 32000 行测量约 69～102 ms，`timing=submit_configuration_to_last_sink_arrival`，包含配置提交/启动而非纯稳态计算。另预先声明仅把输入量增至 320000 的诊断 ABBA（`diagnostic-fresh0-320k`）：12 个有效试次、输出 hash 相同，耗时 641～744 ms，吞吐比 **1.028581**。这支持继续排查短试次/调度波动，但**不能据此证明只是环境噪声或排除实现影响**，也不替代原门禁。后续分离启动时延与较长稳态试次、补调度/CPU 证据后再决定修复或调整验证设计，不能直接降低门槛或重跑挑样本。

TLS/WAN、目标设备、高 key 数/速率容量、24/72 h、介质掉电和任意未声明组合仍为 **NOT RUN/未支持**。当前仍是有限 Preview，未 commit/push/tag 或部署到业务环境。

```text
default server SHA256: eab3efc59a4fe909c444950f0d26b8833c9b1db556b42975977efbb00eaf788d
JetStream server:      de1f4bc5e51640a75a624b7a63c9aeaac5b0095193ab7f0631d03f62a7e6cab0
process driver:        d4d480b32c9c289e109ce5f5fa766c37c1b483284bf8a16815dc0eca6721f1e2
source manifest:       2afc4903b54b430743e2c9cb15529e49ed86352c90bc32612e1104332c7f3053
Mosquitto image:       eclipse-mosquitto@sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21
```

<a id="resample-validation"></a>
## Sampling/Resample：2026-09-26（功能候选，发行门禁单列）

服务器 `box@100.64.0.18`，本批根目录 `/workspace/bench-compare/resample-20260926-XA3hbl`。最终 Rust 候选为 `source-s3`，与本地 `crates/` 清单逐文件核对通过；`package-s3-default/jetstream` 的源清单相同。本机只编辑和格式化，Cargo/Go 编译全部在服务器，后续重复使用冻结可执行文件。基线提交为 `e3bc558`，新增能力尚未提交，不把包内的基线 commit 标记当成干净 HEAD 构建。

- 功能：Last/Mean/Interpolate、typed nullable 输出、缺样与精确网格/左右点边界、完整向量 invalid policy、有界 catch-up、原子恢复和退款、独立 File25/JetStream26、PTC1/TPD1、generation 与 HTTP 输出 ID。模板为 `deploy/pipeline-iot-resample.json`，合同见 [IOT](IOT.md#resample-preview)。
- `full-s3`：Debug 普通测试 **876 passed / 21 ignored**；`frozen-release-s3` 的 Release 全量 **877 passed / 21 ignored**，包含仅在 release 执行的 mailbox 记账和旧 Window future 大小约束。ignored 项不是通过，broker 专项另列。doctest 命令通过，目前没有实际文档样例测试。独立 no-demo **44 passed / 0 ignored**。
- `resample-final2`：冻结的精确 **24 × 20 = 480** 次测试，以及默认包 3 / feature 包 6 个真实进程场景，共 **30 次真实 SIGKILL**，均通过。覆盖 UInt64 完整精度、Mean 黄金值、插值缺右点不外推、已持久化点/等待格在停机期间不推进、未提交输出完整 ID/内容重放、提交后不重复，以及旧 v24 二进制拒绝且 CURRENT/历史/输出不变。
- `resample-transform-final3`：补充驱动 `process-g3` 在两种来源的 Mean 用例中加入前后纯 Project；再次通过两包 9 场景 / 30 次真实 SIGKILL，并重新执行 24 项清单一次。这里只改测试驱动，没有改 Rust 或重编译 Server；生产二进制仍是相同的 s3。驱动源码另存 `driver-g3-source`，不覆盖原 `process-s2` / `source-s3` 的记录。
- `runner-contract-s3`：1 个合法 stub、9 个拒绝反例通过，包含空 guard、错误版本、漏重放/等待格证明、错误强杀数、非零 driver 退出、0 项测试、丢场景和重用证据目录。它只验证编排器，不能作为真实强杀证据。
- 自查与失败记录保留：s1 的 `StateParticipant::freeze_kind` 仍止于 kind12，导致新快照参与者匹配失败；专项测试发现后扩到15，s2复验通过。s2之后自查发现通用 Scalar decoder 会规范化非0 Bool 并分配临时字节 Vec；s3改用新 profile 的严格固定宽解码，并补非规范 Bool key/used、NaN和截断的物化/非物化一致拒绝用例。没有删除失败结果或放宽生产语义。
- 普通 dev-profile Clippy 退出0，111条诊断（不含target汇总）；新模块有 `type_complexity` 风格提示。不是 `-D warnings` 通过，维护性项见 [OPT-009](OPTIMIZATION_BACKLOG.md#opt-009)。

当前匹配二进制 SHA256：

```text
Server default   161505e131c67dd31e816320f9cfb0dcbac82bde8208e1e649efd3ea0a17fe04
Server JetStream a98dcfe0919c03e33542b5b9e2a0b2e18b48ed3b5f61bd1d32109488b8abfa58
process-s2       3ade0a810566c01227fccb2c10909cf0de9548f23fc73a6cbf05ff47a4408a22
process-g3       97b053d66f528e41cb9599dc5848e6992f9a15bf73a8633aeea5be7643bdf1b2
Rust 包源清单    76e5fee1cd77b90b595b14cec0a9f38dff75890a4eb548cfa73014e6bc6f779b
```

**旧链路回归**：告警 `alarm-final2`、静默 `silence-final2` 通过各自专项重复与进程矩阵；`regression-s3/exit=0`。时间图、线性时间组合、paused、参考表/迟滞、A/B1/B2、K1/K3/K4 与 K2 的既有门禁全部通过，使用最终 Release 冻结测试和相同 s3 生产包；K2 单独回收 **35 passed / 0 ignored**，并跑真实 broker 进程矩阵。未匹配的默认 ignored 项不因这句话被补记为通过。

**本候选的性能回归门禁通过**：开跑前写入 `performance-plan-s3.json`，固定原三组 ABBA、fresh ≥0.97、periodic ≥0.90、RSS 增量≤2048 KiB。三个单组均 exit0，合并 `performance-s3.exit=0`，全部输出完整、无丢重/非法行，测量输出 hash 一致；没有降低阈值或重跑挑样本。

| 负载 | 合并吞吐比 | RSS 增量 KiB | 测量数 |
|---|---:|---:|---:|
| fresh 零状态 | 1.006203 | -16 | 36 |
| fresh 双 Count | 1.005961 | 576 | 36 |
| periodic 零状态 | 1.004956 | 116 | 24 |
| periodic 双 Count | 0.999504 | 308 | 24 |

fresh 与历史 time-graph v13 包比较，periodic 为同候选开/关 checkpoint；总体约持平，不宣称统计显著提速。单组最低 fresh 比值为第二组双 Count 的 0.972894，仍满足原 0.97。这只是旧 File 路径回归，不是重采样极限容量、WAN/p99或eKuiper对照。历史 s8 的失败样本仍保留，不能把本次通过改写成旧失败已被证明只是噪声。

上述是限定功能/故障验证，不是 Sampling 极限容量、eKuiper 新对照或生产认证；MQTT live 静默、TLS/WAN、目标设备、24/72h、介质掉电仍未因此完成。逐决策 fsync/full checkpoint/HTTP 成本仍见 [OPT-012](OPTIMIZATION_BACKLOG.md#opt-012)。

<a id="silence-validation"></a>
## 来源观测与静默：2026-09-24（功能通过，性能门禁未整体放行）

服务器 `box@100.64.0.18`，源码与产物分别在 `/workspace/bench-compare/silence-source-20260924`、`/workspace/bench-compare/silence-artifacts-20260924`。本机不 Cargo/Go 编译；源码、测试可执行文件及每次失败证据分别保留。

- `s6` 全量 **852 passed / 21 ignored**；随后补 capability 合同、在健康决策入口拒绝宽限时间溢出、拒绝空来源身份，`s7` 全量 **853 passed / 21 ignored**。忽略项不视为通过，显式 broker/旧矩阵另跑。
- 首试 `package-p1-default/jetstream` 与独立 Go `process-g2`：默认包 2 场景、feature 包 4 场景，共 **30 次真实 SIGKILL** 通过；覆盖 File/JetStream、已观察/登记未见设备、完整输出 ID/内容重放、停机暂停与新宽限、已提交输出不重复、旧 v22 二进制拒绝且历史/CURRENT/输出不变。**p1 是 s6 代码首试，不冒充 s7 最终包证据**。
- 首轮发现并修复 Kernel 把新静默形状按旧 paused profile 预准入的接线错误；另修旧 MQTT→HTTP 演示测试的请求数/记录数混淆（独立提交 `7f41c8d`）。测试自身的类型/时间预期、仍持有输出批次时就断言退款，以及 Go 要求恢复输入前必须额外空闲提交等错误均有原失败日志，未以改生产语义迎合错误预期。
- `frozen-s7` 独立 no-demo **44 passed / 0 ignored**，普通 Clippy exit0、**110 条 warning**；精确脚本的合法 stub 与 19 个拒绝反例通过。脚本拒绝空 guard、丢场景、错误 profile/kind、缺强杀/重放证明、非零退出、0 项测试却 exit0、错误 hash 和重用证据目录；stub 不是实际强杀证据。
- `package-s7-default/jetstream` + 独立 `process-g4` 来源故障验证通过：默认包 5 个独立 fixture，feature 包 7 个，共 12 个，覆盖半行及补全、新完整宽限、删除/替换/截短、慢 required HTTP、自己的 broker 真 SIGKILL/原存储恢复、删除自己的 consumer。broker 断开不会全体静默，重新连接不伪造 resumed。慢 HTTP 的 2 秒 hold 触发合法请求超时重试，完整 ID/内容一致；两个唯一事件最终接受，CURRENT 在等待 ACK 时不前进。`final1` 的原测试把重试请求误当成新事件而失败，日志保留；修正了测试 oracle，未修改生产重试语义。
- 最终候选 `package-s8-default/jetstream`、`process-s8` 与 `final2`：两个 server 与 s7 **逐字节相同**，复用冻结 s7 的 Rust 测试，不重编译测试套件；新包纳入修正后的 Go/脚本。静默精确 **33×20=660**、来源观测 **17×20=340**（每轮含 3 个真实 broker 用例）、**30 次真实 SIGKILL**、上述 **12 个来源故障 fixture** 全部通过。重复与普通套件有交集，不累计为独立用例数。
- Alarm **14×20**、原线性 6 场景和图 4 场景通过；完整旧矩阵通过 JSON、时间图、线性时间组合、暂停时间、参考表/迟滞、A/B1/B2、K3/K4、default/K1、K2。本批完整 K2 清单 **35 passed / 0 failed / 0 ignored**，未使用会漏选的 `--list | grep -q` 管道。旧 paused-time runner 明确排除共享 fixture 的新 observed 子模块，按旧精确清单运行，未把旧合同改成新合同。`validate-final2.exit=0`。

三组原 ABBA 全部完成、样本全保留；全部正确性和输出 hash 一致，RSS 门槛通过，但第一组 fresh/0-state **0.967551 < 0.97**，所以不能宣称性能整体通过。后两组该项为 **1.067795 / 0.992523**。

| 场景 | 三组合并吞吐比 | RSS 增量 | 原门禁结果 |
|---|---:|---:|---|
| fresh / 0 state | 0.998815 | +224 KiB | 合并通过，第一组未通过 |
| fresh / 2 states | 1.008772 | +448 KiB | 每组及合并通过 |
| periodic / 0 state | 1.003277 | +376 KiB | 每组及合并通过 |
| periodic / 2 states | 0.996588 | +56 KiB | 每组及合并通过 |

`performance-final2.exit=3`、`finish-final2.exit=3`，**没有整批 PASS/发行放行**；不改门槛，不用合并值覆盖失败，也未证明失败仅是测量噪声。本批作为功能已验证的限定开发 Preview 保存，性能门禁继续保留为发行阻断项。MQTT live 静默、Sampling/Resample、TLS/WAN、目标设备、24/72 h 和掉电均未因此完成。

匹配证据：源码逐文件清单已在测试前后及本地工作区核对；server SHA-256 为 default `0b475da1d291bc20863e87c0da1155098af80d4c7f4bb235684871776064e2b6`、JetStream `cf846eb0ce0099335c4f8d9c812b0a05fec81bc9e2ebce25e14313c4d6102886`；driver `44c7198e5e01bb22f9fee6f14d235794684b326ebc44b7277117f77a02b1c8cd`。未 push/tag/生产部署。

<a id="alarm-closure-validation"></a>
## 告警子批收尾：2026-09-24（功能通过，性能未放行）

代码基线 `06243b3`（包含 `53e06d2` 的固定大小 Window 单次额度预留），新增验收代码另行提交。服务器 `box@100.64.0.18`，证据根为 `/workspace/bench-compare/alarm-closure-artifacts-20260924`；候选 `package-v1-default/jetstream`、`frozen-v1`、`process-v1`。本机未 Cargo/Go 编译。Rust 与 Go/脚本分阶段构建，Rust/Cargo/deploy/fixture 输入在前后逐文件核对；最终两包拥有同一 source manifest。

- Rust **795 passed / 18 ignored**，独立 no-demo **44 passed / 0 ignored**；普通 Clippy 退出 0、105 条 warning，不宣称零告警。
- Alarm 精确 **14×20**、原线性进程 **6 组**通过；新增 **2 类告警图×2 个生产包=4 组**通过。覆盖双 Alarm 分支合流和双 required HTTP 的部分成功，实际检查 SIGKILL 信号、停机暂停、每 Sink 的独立输出命名空间/连续序号、完整 ID/内容重放、activate/resolve 同 episode、提交后不重复、旧 v19 reader 拒绝且历史/CURRENT/输出不变。两个包均使用 File 来源图，不宣称 JetStream 来源图可恢复。
- 脚本 `scripts/production-alarm-graph-validate.sh` 纳入主 Alarm 验收；`tests/alarm-graph/runner-contract.sh` 的合法 stub 和 8 个拒绝反例通过，仅证明脚本不误报，不代替真实进程测试。
- 完整旧矩阵通过：JSON、时间图、线性时间组合、暂停时间、静态表/迟滞、A/B1/B2、K3/K4、default/K1、K2 broker 与进程。`validate-final1.exit=0`。

原三组 ABBA 未改参数/门槛，全部输出正确且 hash 一致，但**性能整体失败**：

| 场景 | 三组合并吞吐比 | RSS 增量 | 结果 |
|---|---:|---:|---|
| fresh / 0 state | 0.885299 | −64 KiB | 未通过；第 2/3 组为 0.872214 / 0.908143 |
| fresh / 2 states | 1.029827 | +224 KiB | 每组及合并通过 |
| periodic / 0 state | 1.005985 | +396 KiB | 每组及合并通过 |
| periodic / 2 states | 0.998225 | +432 KiB | 每组及合并通过 |

`performance-final1.exit=3`、`finish-final1.exit=3`，没有最终匹配 PASS 标记。此前三组短程双 Count 探测均通过，不能据此覆盖零状态失败；`zero-profile1` 是独立 CPU 诊断，不替代门禁。继续定位公共热路径，不改预算/交付保证、不删除失败样本。静默检测、重采样及目标设备、TLS/WAN、24/72h、掉电等验证仍未完成，不能把本节当作生产认证。

### 后续投影候选 v2：功能通过，仍有单组性能失败

验收代码已提交为 `a5997dd`；其后仅增加完整同位置 Project/Map 的共享批次快路径与回归测试。只允许相同 owner、Reservation、相同列类型/宽度和不收紧 nullable 的重命名；保持原逐行逐步 work 扣减、来源时间，并同原路径清除输出 sequence/operator。计算、重排、截列、Filter、跨 owner 等仍走原路径，未扩大预算。

- 服务器窄验证：Model 57、Runtime 320；新增 10 项各重复 20 轮通过。
- 预先固定的 `probe-p2` 三组 A/A、A/B 全部保留。候选 fresh0 三组为 1.029511 / 1.011308 / 1.006990；fresh2 为 1.018988 / 0.991342 / **0.932071**。A/A fresh0 第三组也有 0.966348 的失败，但不能据此消除候选失败；探测 exit3，不是通过。
- 随后执行一次完整 `v2/final2` 验收：**805 passed / 18 ignored、no-demo 44 passed**，普通 Clippy exit0、104 条 warning；默认包与 p2 字节一致。Alarm 14×20、线性 6 组和图 4 组真实故障通过，旧矩阵 `validate-final2.exit=0`。
- 节点曾短暂离线、SSH exit255；重连后取回原产物，没有将断连当测试失败或重新挑样本。Review 发现临时编排的 `binary --list | grep -q` 在 `pipefail` 下出现 BrokenPipe，可能跳过一个 K2 binary。已用先落完整清单、再选择的方式，对**同一冻结 v2** 补跑完整 K2 32 项（含显式 broker 用例），全部通过，证据为 `k2-inventory-recheck-v2`；旧记录不覆盖。
- 原三组 ABBA 的合并比值为 fresh0 **0.999876**、fresh2 **1.073465**、periodic0 **1.001448**、periodic2 **0.995641**；RSS 增量分别 −28 / +28 / +784 / +184 KiB，全部正确性和 hash 一致。但是 fresh0 第一组 **0.892583** 未过 ≥0.97（后两组 1.006587 / 1.020496），所以 `performance-final2.exit=3`、`finish-final2.exit=3`，**没有最终整体 PASS**。合并改善不能代替每组门禁。

冻结 server SHA-256：default `54647003e6688d2da5df1c42ce79e5ff10565eb71cf8b2eb6b3eb15853b73ea6`，JetStream `39432d46ec0b76699cbe4fb2bb51a5e6d7d09421b9b06259712aeab81283eca1`。仍需定位单组波动；后续固定长样本/同二进制对照仅用于诊断，不覆盖原失败或修改性能门槛。

投影优化已提交为 `7eb57d7`，366 个暂存构建输入与冻结 v2 清单匹配，未夹带下一批来源观测。其后 `zero-long-v2` 按预定三组 A/A、A/B、每次 1,048,576 输入/3 正式轮、CPU affinity 4～7（非独占）完成：A/A 比值 1.011597 / 0.974333 / 1.011627，A/B 1.026605 / 1.132636 / 1.037943；正确性、hash、RSS 均通过。这支持继续调查短程测量敏感性，但不证明先前失败只有噪声，也不替代原 32k 场景门禁。

File/JetStream 来源观测代码不属于冻结 v2，不能继承上述结果。后续静默算子、新持久决策与故障验证的独立状态见 [本批记录](#silence-validation)。

<a id="time-graph-validation"></a>
## 时间型 DAG：2026-09-23 验收

**限定 Preview 的实现、自查和匹配验证已完成，未 commit/push/tag/生产部署。** 新增 required File→HTTP 图的 PT v18 / ET v19，支持持久时间轮、固定物理边序 Union、来源 watermark/idle/EOF 和各 Sink 的稳定输出游标。范围、默认队列/每轮行数限制及嵌入调用者责任见 [DAG 合同](DAG.md#time-graph-recovery)。不包含混合 PT/ET、JetStream 图、参考表、有损支路、历史 replay 或任意图容量。

### 功能、故障与自查

- 最终 `v13`：**774 passed / 18 ignored**，独立 no-demo **44 passed / 0 ignored**；普通 release/all-targets/JetStream Clippy 退出 0，**105 条 warning**，不是 `-D warnings`。
- `final2`：时间图精确清单 **14×20=280**、JSON 严格解码 **15×20=300** 次通过；重复清单与普通套件有交集，不相加宣称独立用例数。
- default / JetStream 两个包分别执行 **6 个真实 SIGKILL 场景 + 1 个默认队列配额拒绝场景**。场景覆盖 PT 双源双 Sink、分支 timer/rejoin、ET 慢源与水位、sealed EOF 正 lateness、hopping EOF，以及明确 idle 后的未提交后继。核验真实信号退出状态、未提交输出内容/ID 一致、提交后重启不重复；多 Sink 场景包含一端已接受、另一端未 ACK。每包另验证旧最高 v17 二进制拒绝 v18/v19，历史、CURRENT 和输出保持不变。
- 旧矩阵全部通过：v16/v17 的 16 个 File/JS 进程场景和 8 个默认 File 场景；paused 25×20 及 6 个进程场景；completion 38×20 与参考表/迟滞进程、兼容 guards；B2 25×20、B1 38×20、Core-A 8×20、K4 50×20、K3 24×20及各自进程；default/K1 smoke、K2 显式 broker 专项及旧进程。
- 自查修复包括：完整 cut 的大小/拓扑/水位一致性、来源完整行指纹的 scalar-only 准入、sealed EOF 后 timer/尾窗存活、Union 整轮 256 行上限及有界固定序重放、每 Sink 独立输出 cut、启动前参与者一致校验。12 边示例超过默认 2 MiB Job 队列预算，保留为拒绝反例，未放宽默认配额。

### 性能：原门槛三组完整 ABBA

在 `box@100.64.0.18` 集中构建和测试，本机未 Cargo 编译。fresh baseline 为上一批 `time-completion-artifacts-20260922/package-v4-default`，使用冻结 K1 driver 与隔离 Mosquitto wrapper；periodic 为候选自身 checkpoint on/off。最终 **三组各自及合并全部通过**，fresh ≥0.97、periodic ≥0.90、RSS 增量 ≤2048 KiB，门槛未变。

| 场景 | 合并吞吐比 | RSS 增量 | 测量样本 |
|---|---:|---:|---:|
| fresh / 0 state | 1.021462 | +336 KiB | 36 |
| fresh / 2 states | 1.068264 | +280 KiB | 36 |
| periodic / 0 state | 1.004164 | +504 KiB | 24 |
| periodic / 2 states | 1.002602 | +460 KiB | 24 |

warmup/measured 全部正确性与输出 hash 一致；periodic 提交成功数为正、失败数为 0。**这是旧高吞吐路径的回归门禁，不是新逐决策 fsync 时间图的容量证明，也不是与 eKuiper 的最新排名。**

性能自查将新增有序 Window future 移到计费的冷构造路径，避免旧路径携带其内联状态；Linux release 的旧窗口 future 从 4328 缩至 3192 bytes。独立 profiling 指向 JSON/分配公共热路径后，将小对象重复键检查改为最多 8 项线性检查，较大对象一次晋升随机 HashSet；保留转义/嵌套/未知字段重复键、深度、尾随内容和类型的严格拒绝语义，未扩大预算或绕过正确性检查。

### 匹配证据与复现

源码：`/workspace/bench-compare/time-graph-source-20260923`；产物：`/workspace/bench-compare/time-graph-artifacts-20260923`。匹配 `package-v13-default/jetstream`、`frozen-v13`、`time-process-v13`，完成标记为 `validate-final2.complete`、`performance-final2.complete`、`finish-final2.complete`。最终逐包/源码/冻结二进制核验记录在 `final-validation.json`、`final-binaries.sha256` 和 `FINAL_MATCHED_VALIDATION_OK`；退出码 0 不能替代这些阶段证据。

```text
default server  49fd529c2ad97e7dfe3de783bc12770f286d63e3f947a40172e14462b81ee9f5
JetStream       3162362789735c86b66cc26491bda40cfddc2f4b3ce2ce6f79e81674a00b7116
Go driver       72b91968490b75b9bd25a42836af50eda0e996272fb2f892ab7cd6b84ddfa204
source manifest 141aab19e995c5e148b6331016e64041ff3f04dbcbefa56b864b9bbe1445c550
build input tgz 5c6ea933591fcb61356cc5f3ae7d898199a775837420430fdc626e5cec99b516
```

本地代码/测试/模板/脚本已与两个包的 source manifest 逐文件核对。包内文档保留构建时快照；此处是后续验收记录，不冒充重新编译。最终文档随 `final-inputs.tgz` 独立归档，不覆盖原始构建输入或失败样本。

复现使用 `scripts/production-time-graph-validate.sh` 及 `tests/time-graph/expected-tests.txt`；Go driver 需共同构建 `tests/k1-k4-reference-process/*.go`，分别传入 default/JetStream 包及旧最高 v17 包，使用 `--time-graph-only`、全新输出目录。完整编排脚本和原始性能 JSON 随产物保留；三组结果分别审核，不能只选合并值。

早期失败保留：测试 fixture 的 schema/If-Match/队列预算/receiver 初始化顺序、idle 场景等待次序以及复用输出路径冲突均有原始记录。被主动中止的旧批次有 `.interrupted`，即使外层退出码是 0 也不计完成。`final1` 的第三组 fresh 双 Count 为 0.962173，虽合并 0.983103 仍不通过；仅隔离 future 的探测及加长采样也未消除失败。JSON 优化后的 `probe2` 和最终 `final2` 才通过，旧失败未删除。CPU profile 中未正常封口/不完整的采样单独标记，不用它们声称性能或 RSS 通过。

**NOT RUN**：真实 TLS/WAN、目标设备、断电/介质损坏、24/72 h 长稳，以及新时间图大状态/多规则持续容量；仍是相应部署/发行门禁。下一批 K4 业务闭环另行验证，不继承本批通过结论。

<a id="linear-time-validation"></a>
## 线性时间组合：2026-09-22 验收

本批新增独立 File v16 / JetStream v17，覆盖 PT tumbling、Change/Deadband 正 TTL 和最多两个状态的线性恢复组合；保留 v14/v15。实现范围与排序合同见 [IOT](IOT.md#linear-time-completion)。构建/测试集中在 `box@100.64.0.18`，本机只做读取、编辑、格式/语法检查，不进行 Cargo 编译。

**限定 Preview 的本批实现、自查与匹配验证已完成，未 commit/push/tag/生产部署。** 工作分支 `feat/core-completion`，基线 `bce8f8b`。不包含时间型 DAG、ET、reference Lookup、side output、Dedup 或任意数量状态；Hysteresis 和 HoldFor/Debounce 自身仍不开放正 TTL。

### 功能、故障与 Review

- `frozen-v4`：**759 passed / 18 ignored**；独立 no-demo **44 passed**。专项按清单显式启用隔离 NATS，ignored 不计入普通通过数。
- `paused-repeat-v1`：**25×20=500**，每轮 24 常规+1真实 broker；`completion-v1`：**38×20=760**。两个清单有交集，不能相加宣称 63 个独立用例。新虚拟时钟 oracle 覆盖 PT/Count/TTL/Debounce 的 15 种有时间状态的两两组合、分段恢复后逐行 payload/ID 等价、下游等时到期先于上游 timer 行、TTL 有效/ignored 输入、Deadband 基线、codec 损坏与额度失败后的原子性/内存归还。
- `time-process-v1-evidence`：File/JetStream × PT、TTL、PT→PT、TTL→Debounce、Debounce→TTL、双 Debounce、HoldFor→Debounce、Debounce→HoldFor，共 **16 种真实 SIGKILL 场景**；`time-default-v1-evidence` 在关闭 JetStream feature 的默认包再跑 **8 种 File 场景**。核验真实信号状态、HTTP 未确认时 CURRENT 不推进、未提交输出内容/ID 相同、提交后重启不重复、停机暂停及适用的 broker ACK cut。旧最高 v15 二进制对 v16/v17 的拒绝保持历史、CURRENT 和输出不变。
- 旧时间 profile：`paused-process-v1` 六种 File/JS 进程场景及 `paused-default-v1` 三种默认 File 场景通过；旧参考表/迟滞九种进程及兼容 guards 通过。B2 25×20、B1 38×20、Core-A 8×20、K4 50×20、K3 24×20及对应进程通过；K2 32 项（含显式 broker）与两个真实进程、default/K1 smoke 通过。
- 普通 release/all-targets/JetStream Clippy 退出 0，**103 条 warning**；不声称 `-D warnings` 通过。提交前级别的 whitespace、脚本语法、新测试格式检查完成。
- 自查重点：有序时间先于下游衍生行、每个实例独立恢复、PT 半开窗口边界、TTL timer/index 的预算和重建、激活前的 cut/schema 验证、旧 profile 保持及 live restart_fresh 不变。回归发现并修复了 live TTL effective metadata 被错误标成持久恢复的诊断回归；没有把旧 fresh 语义改成 paused。

### 性能：三组预先声明的完整 ABBA

沿用冻结 K1 driver 和隔离 Mosquitto fixture，fresh 对照为 **9 月 19 日已验收的 `package-v7-default`**；periodic 比较候选自身的 checkpoint on/off。三组每组及合并均通过原门槛：fresh ≥0.97、periodic ≥0.90、RSS 增量 ≤2048 KiB，未降低门槛。

| 场景 | 合并吞吐比 | RSS 增量 | 测量样本 |
|---|---:|---:|---:|
| fresh / 0 state | 1.009588 | +36 KiB | 36 |
| fresh / 2 states | 1.006952 | +24 KiB | 36 |
| periodic / 0 state | 1.004455 | +328 KiB | 24 |
| periodic / 2 states | 0.996858 | +240 KiB | 24 |

全部输出 hash/数量校验一致；periodic 提交成功数为正、失败数为 0。**这是旧高吞吐链路的回归门禁，不是 v16/v17 容量认证。** 旧 v14 的单 key/leading-only/100 行串行成本复测约为 152.7 行/s（无人工 HTTP 延迟）、37.9 行/s（20 ms 模拟响应延迟）；这既不是真实 WAN，也不能外推至新双状态、大状态或多规则。新 profile 仍受 [OPT-012](OPTIMIZATION_BACKLOG.md#opt-012) 约束。

### 证据与复现

源码：`/workspace/bench-compare/time-completion-source-20260922`；产物根：`/workspace/bench-compare/time-completion-artifacts-20260922`。`package-v4-default/jetstream`、`frozen-v4`、`validate-v1`、`performance-v1` 与逐阶段日志互相匹配，最终核验为 `final-validation.json` / `FINAL_MATCHED_VALIDATION_OK`；不只凭 SSH/外层脚本退出码判断完成。

```text
default server  b846921e62d7c4ebfdeb381fcb77c4fee91af050665cc1a01c0f0adea730d5b1
JetStream       096b6c87afd0c4070c42e5ea082dbcb3e8fdff4e280b24145b36c56a1c4d0989
Go driver      04000545b16c1e1b20a4ce6ffe60bf78b9f1e8c79e90549387590c6e69636207
source manifest 9f2fc68b1fe3b8e50a5a731336bb3e9329b0327886d7ba4e8e043628b09c89f6
build input tgz 12c865a8e78c0e881b70ab8b0c3b52c52b753ebc0e21b17a705fd149cb58a67e
```

本地受指纹覆盖的源码、测试、模板和脚本已全部与两个生产包 manifest 核对一致。包内文档保留构建时状态；本仓库最终文档是后续验收补充，不冒充二进制重编译。最终输入归档另保存为 `final-inputs.tgz`，不覆盖早期输入/失败记录。

复现时在 Linux 构建 `tests/k1-k4-reference-process/*.go` 的三个文件，使用 `--time-completion-only` 或 `--time-completion-file-only`，传入 `--server-bin`、`--old-server-bin`、`--nats-server`、全新 `--out`。冻结重复使用 `production-paused-time-validate.sh` 和 `production-k1-k4-completion-validate.sh`；性能用原 `production-k1-performance.sh`，三个完整 ABBA 组分别保留，禁止只挑合并通过的样本。

保留失败：v1 新测试引用了未从 crate root 导出的 WindowOperator；v2 修正上述 live TTL metadata 及两处旧拒绝文案断言；v3 更新仍然不合法的 Log/no-directory PT 配置之 API 错误断言。v4 全部通过，不删除此前失败，也不把 v2 的九项局部成功冒充全量成功。

**NOT RUN**：真实 TLS/WAN、目标设备、24/72 h 长稳、断电/介质损坏、新时间 profile 的大状态/多规则持续容量。它们仍是相应部署/发行门禁；三项开发完成不等于全场景生产认证。

<a id="paused-time-validation"></a>
## 可恢复时间首批：2026-09-19 验收

**限定 Preview 已完成本批验证，不是完整 K1～K4 或生产认证。** 仅单个线性 HoldFor / Debounce，File append-only v14 或可选 JetStream v15，required HTTP、暂停的逻辑时间、CURRENT-only 恢复。每个输入行/空闲 tick 先持久决策再执行、每决策 checkpoint；不开放 PT Window、正 TTL、多状态时间组合、时间型 DAG、完整告警生命周期。合同与备份边界见 [IOT.md](IOT.md#paused-time-preview)。未 commit/push/tag/生产部署。

全部 Rust 构建在 `box@100.64.0.18` 集中执行，本机未编译。该节点为 Debian 13.6、Linux 6.12.94+、8 vCPU Intel Xeon；不直接继承原 `.16` 的性能数字。

### 功能、故障与兼容

- `frozen-v6`：747 passed / 18 ignored；独立 no-demo 44 passed。18 个 ignored 不计入普通通过数；本批 NATS 和既有 K2 等由专项明确启用。
- `paused-repeat-v6`：精确清单 22 项 × 20 轮 = 440 次通过，其中每轮 21 常规 + 1 隔离 NATS。覆盖配置、算子、完整行/NULL、timer/预算、codec、日志完整性、停机暂停、CURRENT 故障、慢 HTTP 重放和缺失日志拒绝。
- `process-v4` / Go driver-v4：File、JetStream 各执行 HoldFor、Debounce trailing、Debounce leading，共 6 种真实 SIGKILL 场景；核验真实信号退出状态、未提交输出 ID/内容完全一致、已提交后重启不重复、停机不计时及未提交输入不 ACK。上一批 package-v7 二进制（最高支持 snapshot v13）对 v14/v15 的两个拒绝反例保持完整历史、CURRENT 和输出不变。
- `process-default-v5` / Go driver-v5：默认关闭 JetStream 的生产二进制再跑上述 3 种 File 进程场景，全部通过。
- 旧矩阵：参考表/迟滞 26×20 + 9 种进程及旧 profile guards；B2 25×20、B1 38×20、Core-A 8×20、K4 50×20、K3 24×20及其进程；K2 专项（含真实 broker）、真实进程与 default/K1 smoke 均通过。
- 普通 release/all-targets/JetStream Clippy 退出 0，102 条 warning；不声称通过 `-D warnings`。

### 性能：相同节点重新跑原门槛

`performance-v3` 使用原冻结 K1 driver、原 K4 v5 baseline 和新 default package；三组预先声明的完整 ABBA **每组及合并均通过**。fresh 比值门槛仍为 ≥0.97，periodic on/off ≥0.90，RSS 增量 ≤2048 KiB；未降低门槛。

| 场景 | 合并吞吐比 | RSS 增量 | 测量样本 |
|---|---:|---:|---:|
| fresh / 0 state | 1.003825 | +820 KiB | 36 |
| fresh / 2 states | 0.990209 | +1016 KiB | 36 |
| periodic / 0 state | 1.005042 | +400 KiB | 24 |
| periodic / 2 states | 0.999532 | +56 KiB | 24 |

全部样本输出校验通过；periodic 提交成功数均为正、失败数为 0。这证明本轮原链路门禁未退化，不代表新时间型串行 profile 有同等吞吐。

新模式另做成本观察：File、单 key、Debounce leading-only、逐行 POST，各 100 行全部提交、100 个连续输出 ID；无人工 HTTP 延迟约 **139 行/s**，20 ms 模拟响应延迟约 **34.1 行/s**。这不是 WAN 或持续容量认证；大状态、多规则、磁盘写入量未外推。后续批处理/group commit 见 [OPT-012](OPTIMIZATION_BACKLOG.md#opt-012)，不能删除持久时间/ACK 保护换吞吐。

### 来源与保留的失败样本

产物根：`/workspace/bench-compare/paused-time-artifacts-20260919`；对应 `package-v7-default` / `package-v7-jetstream`、`frozen-v6`、`paused-repeat-v6`、`process-v4`、`process-default-v5`、旧矩阵 `*-v1` 和 `file-performance-v3-*`。测试源码、日志、原始失败和 checksum 均保留。

```text
default server  22f8be3f6ccb098da1b4e3ad076e1589b61b4a78be9c4ad956a16c5e1396b74a
JetStream       fc13de3e162fdb99aa533249a5fd9b3bbf7e04701b48de8a2281beb5ea505a9a
source manifest 4e49381c2159a7fbf7295635c25baa18c65fccdd3771512b3d38a4df8835e930
Go driver-v4    a629af3ba052f16b9b5d2a437144031608c9732c911619e36fa1c9961a7ea803
Go driver-v5    4539f567433c161ac92b20ddc1396208f0810f7d413732e27fb5d6b52ec8c21d
```

本地逐文件复核中所有 Rust 与生产包 source manifest 一致；两个 Go 文件是构建后独立冻结/验证的 driver 更新，不能称为原 manifest 的同一版本。Go 校验器现在包含 `main.go`、`stateful_dag.go`、`paused_time.go`，必须一起构建。原包保留构建时文档；此处最终记录是后续证据补充，不冒充重新编译。

保留：初始归档遗漏 workspace/打包目录、测试类型标注与不适用的 Cargo 参数失败；SSH 中断后的 `regression-v1.exit=0` **不作为全程通过证据**，K3/K2/smoke 后续阶段按实际完成日志补齐。Go driver-v3 在快提交时把 `CURRENT→PUBLISHED` 发布窗口的 ENOENT 当成失败；v4 仅在固定期限内重试 ENOENT，CRC/格式错误仍立即失败。性能 v1/v2 因新节点缺 Mosquitto/共享库而在测量前失败；在任务目录解包 Mosquitto 2.0.21 及依赖、校验 `ldd` 后运行完整 v3 三组，未安装/启动系统服务。原 driver 仅通过 wrapper 增加隔离 broker 路径；具体依赖 hash 见 `performance-v3-dependencies.sha256`。

**未运行/未覆盖**：真实 TLS/WAN、目标设备、24/72 h 长稳、断电/介质损坏、时间型大状态/多规则容量。上述项目仍是对应部署/发行门禁，不因短程通过变为已认证。

## 构建与追溯

```sh
bash scripts/production-build.sh /absolute/new/package
cd /absolute/new/package && sha256sum -c SHA256SUMS
```

脚本分别构建 `sparrow-server` 与 `sparrowctl`，显式 `--target x86_64-unknown-linux-gnu --no-default-features --locked --release`，不与 demo 包混合 feature。构建、依赖树与复制路径使用同一显式 target，不受继承的 `CARGO_BUILD_TARGET` 或 Cargo 默认 target 干扰而误打包旧 host 二进制。保存依赖树、工具链、逐文件源码指纹、二进制/部署文件校验和；包内 manifest 使用可迁移的相对路径。`source_commit` 不代表工作区干净，需同时检查 source manifest 和匹配测试证据；正式发行要求干净提交。历史示例使用默认 features，或 `--no-default-features --features examples`。CLI 生产依赖不含 Kernel/testkit/connectors。

CI 分核心回归、独立 no-demo 测试/真实进程 smoke、周期确定性重复。周期重复不等于 24/72 小时长稳。真实 RTT、介质/断电、目标硬件与长稳是独立发行门禁。

默认 `SPARROW_BUILD_MODE=candidate`。正式包使用 `SPARROW_BUILD_MODE=release`，脚本要求构建源码、README 与 docs 已提交且无未跟踪文件（不包括根目录 review/过程 MD）。默认不打包原始本地 diff；只有显式 `SPARROW_INCLUDE_SOURCE_PATCH=1` 才附带受限源码路径的 tracked patch，使用前人工检查凭据/私有配置，且该 patch 本身不包含新文件。源码指纹不是脱敏工具。

可复现冻结与重复（同一源码、同一工具链；两个 feature profile 不混用）：

```sh
bash scripts/production-freeze-tests.sh /absolute/new/test-evidence core
bash scripts/production-freeze-tests.sh /absolute/new/test-evidence production
bash scripts/production-repeat.sh /absolute/new/test-evidence 20
```

冻结脚本一次测试后保存 Cargo 返回的实际 executable、SHA256 和日志；重复脚本不编译、不覆盖旧证据。CI 定时任务使用同一脚本/筛选范围。`git diff --check` 比较 base 与 HEAD；clippy、包校验和、脚本语法检查为门禁。全仓 rustfmt 历史欠账暂单独输出 `format.log` 与 CI warning，**尚非阻断门禁**，不为本轮修复混入无关全仓格式变化。

## 运行目录与安全

- `/opt/sparrow/releases/<build-id>`：只读、校验过的完整包；`current` 指向选定版本。
- `/var/lib/sparrow/catalog.db`、`data/`、`checkpoints/`：专属 `sparrow` 用户，仅此目录可写，不与其他用户/程序共享写权限。
- `/etc/sparrow/production.env`、`secrets.key`：0600。systemd 读取的 env 文件可由 root 持有；**key 文件须允许实际服务用户读取**，例如 0600、owner=sparrow，父目录使用适当的 root:sparrow 0750。密钥为 32 原始字节或 64 个十六进制字符，长期保存，备份与 catalog 分开管控；缺失/错误密钥不得用“生成新密钥”掩盖。
- 模板见 `deploy/`；创建目录与服务用户后自行安装并 review systemd 单元。模板不会自动部署；必须替换 token，并使用 schema 与实际设备匹配的 stream。
- 管理端默认 loopback；跨机使用可信 TLS 反向代理或 SSH 隧道。Server 本身不提供管理 HTTPS 监听。`sparrowctl` 默认拒绝远端明文、URL 内凭据和重定向；明确 `--allow-insecure-http` 才允许远端 HTTP。
- `sparrowctl` 的服务 URL 必须挂在根路径 `/`，不支持代理子路径；HTTPS 当前使用 webpki 公共根，无自定义私有 CA 参数。私有 CA 环境可用 SSH 隧道连接 loopback，不能用跳过证书校验代替信任配置。非 demo Server 拒绝模板占位 token、少于 16 字节、全同字符或非可见 ASCII token；这不是随机性证明，仍应注入真正随机密钥。兼容保留的 Server `--token` 会出现在进程参数中，不推荐使用。
- Connector TLS 校验证书；网络目标仍须配置现有 allowlist，SecretRef 引用不能替代授权。文件/checkpoint 受 `SPARROW_DATA_ROOTS` 限制。safe-mode 缺少明确 root/持久密钥时拒绝。
- catalog 与 checkpoint 使用协作式文件锁；锁文件不删除。此合同面向可信本地文件系统目录，不提供对恶意同权限进程、硬链接别名或不可靠网络文件系统的隔离。
- SIGTERM/SIGINT 停调度并等待任务/阻塞提交退出。磁盘 I/O 永久阻塞时没有“必定有限时间优雅退出”的保证；systemd 最终强杀属于异常退出，不算 flush 成功。
- 收到停机信号先置 draining 并通知 HTTP 停止 accept，新到达的非 GET/HEAD/OPTIONS 请求在鉴权后返回 503；此前已准入请求仍可能完成。HTTP 排空后等待任务 join，超过 120 s 仅告警并继续等待，不 detach/abort 阻塞提交。systemd 的 `Restart=on-failure` 是进程层重启，与 Supervisor 的 pipeline 退避重试不同；固定恢复点保护同样适用于进程重启。

## 最小操作闭环

```sh
export SPARROW_URL=http://127.0.0.1:43180
# SPARROW_TOKEN 由受控环境注入，不放到命令行或 shell 历史。
sparrowctl health
sparrowctl capabilities
sparrowctl put-stream sensors stream.json
sparrowctl validate deploy/pipeline-aligned.json
sparrowctl explain deploy/pipeline-aligned.json
sparrowctl put-pipeline sensors deploy/pipeline-aligned.json
sparrowctl start sensors
sparrowctl status sensors
sparrowctl checkpoint sensors
sparrowctl checkpoints sensors
sparrowctl diagnose sensors --output new-diagnostic.json
sparrowctl stop sensors
sparrowctl restore sensors --snapshot-id 1
```

更新已有配置必须带服务器返回的 `--if-match ETAG`。start 的成功表示 desired 已提交，不等于实际 ingress 已激活；继续检查 status 的 revision/attempt/健康和进展。CLI：0 成功，1 本地/网络/超时错误，2 HTTP 拒绝。输入 JSON 上限 64 KiB，响应 2 MiB，诊断 128 KiB；诊断文件独占创建、0600，不覆盖旧证据。诊断只导出 allowlist 状态/有限审计摘要，不含 raw spec、SQL、目的 URL、SecretRef、自由文本错误/日志或任意宿主文件；不是完整日志归档。

restore 在一个 catalog 事务内同时发布含 RestoreSpec 的新 revision 与 desired=running；HTTP 处理器不再先 kill 再单独 start。返回成功仍是异步请求被接受，恢复点兼容性与实际启动需继续检查 status。Supervisor 统一执行旧任务 join 后再替换；已准入的启停/收敛操作由最多 128 个独立生命周期工作持有锁和子任务，即使 HTTP 或嵌入调用者取消等待，也不会提前释放转换锁、让新旧任务交叠。取消等待不等于撤销操作。

诊断事件是“全局最近 64 条审计中属于目标的最多 32 条”，响应明确给出窗口范围；空 events 不代表目标没有历史事件。反向代理返回非 JSON 的非 2xx 响应也按 HTTP 拒绝退出 2；成功响应仍须合法 JSON。`--output` 创建/写入失败时，已取得的诊断 JSON 回显 stdout 并退出 1；可能留下不完整的新文件，不能视为完整证据，也不会覆盖已有文件。

## 周期 checkpoint 与恢复

`checkpoint` 可省略，默认仅手动、不自动重放。配置项：

| 参数 | 默认 | 合法范围 |
|---|---:|---|
| interval_ms | null（关闭） | 100～86400000 |
| timeout_ms | 5000 | 100～120000 |
| retain_generations | 3 | 1～128 |
| max_store_bytes | 33554432 | 1 MiB～1 GiB |
| resume_latest | false | bool |

K1 支持单 File/replay → 线性计算 → 单 required Sink 的零状态、单 Count/ET tumble/ET hop，以及两个 Count 窗口串联。双状态混合时间策略、超过两个状态、PT/Dedup/Lookup、分支/多源/MQTT 入口恢复仍拒绝。进入 state 的 key 和 MIN/MAX/FIRST/LAST 值须由现有 Scalar snapshot codec 支持，nested/Dynamic 状态值不开放；无状态计算或 COUNT(*) 的无关宽列不因此一概拒绝。每个 attempt 仍只有一个排队/进行中请求，手动和自动共用 gate。

每次切点必须收齐当前 attempt 的 Source、全部状态实例、required Sink ACK；零状态不是免除 Source/Sink 责任。实例以 operator/slot/shard（当前 slot=1、shard=0）区分，相同 ACK 幂等，冲突/未知 ACK 拒绝，旧 attempt/旧 checkpoint ACK 不补齐新切点。只有全部 freeze 和真实 Sink flush 成功才能发布 CURRENT；prepared ACK 集合及已编码快照不能由外部调用者任意构造为“可信”。

恢复前先校验完整参与者集合，再在同一 Job owner 下准备所有窗口；缺失/重复实例、错误 accumulator/key 类型或边界、预算不足均在 Kernel 输入激活前拒绝。`max_state_keys` 仍是**每个算子**的上限，不因两个参与者静默减半；最多两个参与者限定总条目工作量，完整 snapshot 总上限仍为 8 MiB，所有参与者共享 Job 的 reservation/retention，不各得一整份预算。

`append_only` 的空文件/暂无追加继续保持可 checkpoint；全过滤也推进真实 source cut。`sealed/immutable` 保留终止水位后完成 Job 的既有行为，不为 K1 改成永久 Running；需要 checkpoint 必须在活跃期间完成，不能把已经完成的有限作业宣称为仍可接收新 checkpoint 请求。

状态实例身份与 snapshot 序号不同：fresh/reset 在任何输出前，用 OS 安全随机源生成并持久化 128-bit `STATE_GENERATION`（`SG01`）；写入/同步失败不激活 Kernel。兼容恢复沿用快照里的 generation，配置 revision 和执行 attempt 可以变化。`checkpoint.state_generation` 为 32 位十六进制标识；随机碰撞概率约 2^-128，不是时钟/进程号拼接。该 marker **不是 CURRENT/提交证明**，不用于跳过源码、状态或输出完成校验；K1 尚不把它当作已经实现的业务输出 ID 或 exactly-once。

快照 v3 保持 `CPL1` 外层参与者清单，在旧 reader 可读取的 `CP01` 语义字段中使用 `RCP2` 封套保存完整诊断计算和状态依赖前缀；其余包含 source cut、各 state frame、attempt/revision/generation。恢复必须匹配 Source 身份/schema、每个状态实例及其全部上游计算（仍保留全局函数语义版本检查）；线性管道比较到最后一个窗口。末端下游 Filter/Map/Project 可以调整，零状态管道可调整过滤/投影而不重放文件头，状态中暴露 `downstream_semantics_changed`。两窗口之间的计算仍影响第二窗口，窗口参数/schema/参与者变化仍拒绝。改变输出逻辑不撤销已发送的 HTTP 副作用，也不提供 exactly-once。

**v29/v30（补充聚合，第11批子批1）：**外层沿用 CPL1，含 FIRST/LAST/VAR_*/STDDEV_* 的窗口参与者使用 participant codec 3；窗口 frame 与 v3 相同，累加器新增 tag 8（Value：mode、has、值）和 tag 9（Moment：mode、n、mean、m2）。v29 = File 线性、单个 Count/ET 滚动/ET 跳跃窗口，或两个 Count 窗口；v30 = JetStream 线性、1～2 个 Count 窗口，输出游标与状态同切点。两个窗口中含 ET 的组合暂不支持。完整计算语义及依赖严格匹配，**不适用 RCP2 下游放宽**；不兼容明确拒绝，不自动清空状态、不回退旧 checkpoint。codec 1 frame 在编码、解码和仅校验扫描三条路径都拒绝 tag 8/9。目录 profile 隔离与 v3～v28 相同：v29/v30 不与旧版本混写，不从 v3/v4 迁移，旧二进制在 profile 层拒绝。codec 3 绑定当前顺序 Welford 算术；算术改动必须换 codec/profile。

**v31/v32（滑动计数，第11批子批2a）：**外层沿用 CPL1，滑动计数 `COUNT_WINDOW(size, step)` 的窗口参与者使用 participant codec 4（`BWF1`）：头部与窗口 frame 相同（operator、slot=1、kind=5、key 数），之后为 `BWF1` 标记与 size，再逐 key 写 key 值、该 key 的到达序号 n 以及最近 min(n, size) 条已求值、已分离的聚合输入（序号 + 值），**不存累加器、不存原始行**。解码在扫描和物化两条路径都校验：key 数 ≤ max_state_keys、保留条数 = min(n, size)、序号为以 n 结尾的连续尾段、标量 tag 合法；恢复时再按活动算子校验 operator/size/key 与输入类型、key 严格有序无重复。v31 = File 线性；v32 = JetStream 线性 + 输出游标（epoch = state generation）。仅允许**单个**滑动计数窗口作为唯一状态；DAG、参考表、IoT、ET/PT 状态、侧路、source-time、nested/Dynamic 聚合输入均拒绝。完整计算语义严格匹配（size/step/聚合/下游别名任一变化即拒绝），**不适用 RCP2**；不兼容和额度不足带 `checkpoint_guard`，不回退旧代。目录 profile 与其他版本隔离。滑动计数 key 不过期，仅受 max_keys 约束（Q7）。恢复额度 = 每 key（4096 + 4×key 值驻留）+ 每条输入（1024 + 值个数×Scalar + 值驻留）+ 128，扫描阶段由编码字节精确算出；v31/v32 运行中同一算子按同一规则持有 Retention（先按求值前估算准入，求值后调整为精确值），所以恢复后的占用与切点时相同（Q4）。

**v33（ET 滑动 / ET 会话，第11批子批2b）：**`SLIDING(ts, size[, delay])` 或 `SESSION(ts, gap, max_duration)` 作为 File 线性管道的**唯一**状态时开放 aligned 恢复。participant codec 4 沿用 `BWF1`：头部 kind=7（ET 滑动）/9（ET 会话），`BWF1` 后为 (size, delay) 或 (gap, max_duration)，逐 key 写 key 值、到达序号 n、保留输入（事件时间 t、序号 seq、pending、已求值聚合输入），末尾为单输入水位生成器状态（activity、wm、max_event_time、last_effective）。**不存** schedule/deadline/Key 编码/字节计数（恢复时按在线同一规则重建）。扫描与物化两条路径校验派生范围：参数满足 `WindowKind::sliding/session` 构造约束；1 ≤ 保留条数 ≤ n；t ≥ 0；(t, seq) 严格递增、seq ∈ [1, n] 且不重复；pending 只出现在滑动且已触发的输入全部在 pending 之前；wm ≤ max_event_time、last_effective ≤ wm、有状态必有 max_event_time、所有 t ≤ max_event_time。恢复时再按活动算子校验 kind/参数/max_buffered_rows/类型，以及**派生不变量**：线性 File 输入不会 idle、`wm = max_event_time`（线性 ET 的 out_of_orderness 恒为 0，仅图 source_time 才可设置，而 v33 拒绝 source_time）、`last_effective = wm`、切点时无到期项（每 key 的输出 deadline > wm）；任一失败为 `buffered_state_mismatch`，不回退旧代。v33 manifest 严格匹配完整语义（kind、size/delay 或 gap/max_duration、max_buffered_rows、keys、aggs、event_time_field、future skew、下游计算），**不适用 RCP2**；与 v31/v32 互认失败均为 `buffered_profile_mismatch`（含伪造的 RCP2 前缀、外层版本改写、来源 kind 改写）。旧/非持久编码路径（SPV1、restart_fresh 窗口）对 ET codec 4 一律拒绝，含空状态。**JetStream + ET 继续拒绝（S2 Q6）**；PT 滑动/会话/跳跃仍为子批2c。恢复额度规则与 v31 相同（key 4096 + 4×key 驻留；输入 1024 + 值个数×Scalar + 值驻留；+128），v33 运行中同规则持有 Retention。
**v33 已知限制（文档化，不改语义）：**(1) future-skew 判定 `ts > now + skew` 的 `now` 是重放时的宿主墙钟（与 v3/v29 相同，S2 Q3）：原本被判为 future 丢弃的近未来行在重启后可能被接受，测试数据远离该边界；(2) 线性 File 没有 idle 生成器，**ET 窗口只在新事件推进水位或 EOF 时关闭**；恢复后若无新输入，未关闭的会话/滑动窗口不会输出（append_only 文件不产生 EOF）；(3) Session 保持 final-only、L=0，不做迟到更正/撤回；(4) 下游 Watermark 控制在恢复后的首次推进会以相同值再发一次（非语义，单调不回退）。

**恢复内存预留（第11批 Q3，适用全部版本）：**所有 Server 恢复入口统一经 Store 的 owned 解码入口：读 chunk 前按 MANIFEST 字节取 payload 额度 → 有界扫描与校验（逐 frame 计入解码临时数据）→ 按扫描得到的每个参与者精确驻留字节（状态、索引、key/值）预留 → 物化 → 额度随 `RestoreCredit` 移交给 Kernel 的同一 Job owner，所有参与者准备成功后才激活输入。任一步失败完整退款，CURRENT 不变、不启动输入、不产生输出；额度不足（`checkpoint_guard=restore_credit`）与 profile/codec 不兼容一样属于不可回退错误，不当作损坏去选更老的代。旧格式和计算语义不变；低预算下比以前更早拒绝属于预算修复。已审计入口：File 线性 v3～v28、paused/observed/graph 时间 profile、File DAG、JetStream 各 profile、legacy SPV1 `restore_with_table`、可靠 Sink 目录历史扫描（计入 Sink owner）。剩余缺口：提交前读取旧 CURRENT 的校验扫描不是恢复入口，仍只做有界读取；嵌入方自行解码后调用 legacy Kernel `adopt` 的路径在解码后才取额度（Server 不使用）。

历史 K1 plain `CP01` 快照仍可读取，但保持原来的**完整计算严格匹配**合同；需要下游更新时，先用原计算在本版本提交一个 RCP2 恢复点，再更新配置。旧 K1 可以完整解析新 CURRENT 的外层结构，但必须在计算兼容检查明确拒绝，不把未知 manifest 当损坏、偷偷回退到更老的 plain CP01。未发布的 R11 开发 CPL2 不属于支持格式，R12 移除该只读分支。R10 SPV1 v1/v2 不自动迁移，Server 在输入激活和 generation 写入前拒绝向含旧格式历史的目录写 v3，包括 `resume_latest=false`。不要靠删除旧代绕过检查；选择新目录 fresh 或恢复原二进制和同一备份时点的整套目录。

**R10 回滚限制：**旧二进制遇到混合 v2/v3 目录可能跳过 v3、恢复旧 v2 并重复输出；“不兼容即拒绝”仅在没有可回退旧代的单 codec 目录得到旧二进制保证。K1 的 guard 阻止正常 Server 创建这种混合目录，但不能修补外部工具或历史版本已经混写的目录。回滚必须先核对全部历史 codec，在副本上验证，不能只看 CURRENT。

切点捕获与 barrier 注入保持同一 Source 驱动内的 FIFO 顺序；之后 Source 可继续读取。单飞 checkpoint worker 独立收齐 ACK，再在 blocking pool 编码/提交，不要求 durable outbox，也不跳过 required Sink flush。停止、读失败和 EOF 都收尾该 worker；已经进入阻塞提交的工作不能因 API 超时/取消而 detach，持有 gate、lease 和目录锁直到结束。满队列时 worker 仍独立被调度，不依赖 Source 的批次 send 返回。

Source 不再停读也扩大了可能重复输出的窗口：Sink barrier 完成后可以发送 cut 之后的数据，而 CURRENT 仍在提交；此间崩溃会从上一提交点恢复，期间已发出的外部输出也可能重放。这没有改变 at-least-once 边界，不是 exactly-once；外部接收方需要幂等。

`checkpoints`/`diagnose` 同时展示 `STATE_GENERATION` marker 与各快照的版本/revision/attempt/generation，允许识别同一 v3 目录中的 fresh lineage。头部元数据仅作有界诊断，运行中缓存于启动/最近完成提交；不代替 CRC、MANIFEST、PUBLISHED 和兼容性校验，未知/损坏头部为 null。marker 不是恢复依据，兼容恢复相同 marker 不重复写盘。

**旧 codec 不自动迁移**：K1 Server 拒绝 R10 的 SPV1 v1/v2；R10 也不能恢复 v3。底层 Store 仍识别旧 codec 的完整性以安全管理旧历史，但不把格式不兼容当成损坏，偷偷回退成一个旧格式/旧位置。升级需保留原二进制和整套备份，或明确选择新目录 fresh；没有在线/离线自动转换工具。旧 `CheckpointSnapshot`/`PlanLayout` 和 legacy barrier helper 仅保留嵌入单窗口合同；Server 的零/单/双状态统一走 `PipelineSnapshot` / `PipelineRestore` / `wait_participants`。没有开通 DAG 或 broker 无损恢复。

嵌入 API 变更：已有 `AlignedJob` struct literal 需显式补 `pipeline: None` 才保留 legacy 路径；K1 使用 `pipeline: Some(PipelineRestore { plan, generation, restore })`，不同时填写 legacy `restore`。非 Server 的组合层必须自行持久化 fresh generation、提供有序 cut 和真实 Sink 完成计数；Kernel 不替外部来源伪造可重放保证。

HTTP/CLI 等待超时不撤销已开始的阻塞持久化；gate 持有到 source actor 真正完成。检查 `checkpoint.active/phase/last_success_id/timed_out_waiters_total` 再决定重试。周期不是硬 RPO，`last_success_age_ms` 是本 attempt 成功提交后的单调时钟年龄，恢复旧点时为 null，不伪造历史时间。磁盘信息是有界快照，不能当作跨组件事务快照。

Sink flush 共用该次 checkpoint 的端到端 deadline，不另设固定 5 秒截止；barrier 请求释放后立即解除其 flush 等待，不把已放弃的 barrier 留在数据通路上。此取消不会伪造 outbox ACK 或清除实际丢失，且不改变已进入阻塞提交后必须等真实完成的合同。

运行中的 `/checkpoints` 使用启动/最近完成提交的缓存，明确返回 `storage_sample`；不会为监控持有 writer 文件锁而阻止新 attempt。停止后返回实时磁盘列表。怀疑失败留下临时文件或外部磁盘变更时，先停止并重新列表，不把运行缓存当成实时磁盘扫描。

`CURRENT` 持久化是提交边界；`PUBLISHED` 记录已发布证明。损坏回退只选有证明的旧点，不能拾取未发布 MANIFEST。历史 CURRENT 在替换前补证明。GC 保留 CURRENT/最后有效点并限制代数与逻辑文件字节；失败后可能多留一代并报告 maintenance error。逻辑字节不含文件系统元数据，不等于磁盘配额，仍需独立文件系统空间告警。只删除规范 checkpoint 代目录，不删未知文件。列表不修改 CURRENT，也不宣称兼容；完整 CRC/codec/语义/身份在恢复时检查。列表与选择不是 pin 协议，选点前先 stop 并等待无活跃任务，避免自动 GC 与人工选点竞态。

`resume_latest=true` 明确允许启动/重启时从提交点恢复；仅真正空历史可 fresh。损坏/缺 CURRENT、有历史但无有效点、旧 codec、变更 schema/Filter/Map/窗口语义或 File 替换/截断都会拒绝，不静默重头读。`append_only` 文件不得 truncate/rename 替换，轮转需要停机建立新源/新 checkpoint 目录。sealed/immutable 文件不可修改；具体身份失败见结构化错误与本地受控日志。

数值 `snapshot_id` 是该配置的持续恢复依赖，后续 start 仍指向它，不等于“以后自动用最新点”。运行中 GC 会 pin 该点；代数至少保留 pin + CURRENT 两代，即配置 retain_generations=1 时有效下限为 2，字节上限仍有效。希望以后追随最新点时，发布去掉固定 restore 的配置并明确 `resume_latest=true`。每条 pipeline 使用独立 checkpoint 目录；不支持多条停启配置轮流共享目录的依赖管理。

R10 对数值恢复点采用 **fail-closed**：运行失败或 Server 进程重启后进入 held，即使非 safe-mode 也不自动从旧 pin 重放；操作员确认重复输出风险后显式 start 才解锁，仍使用原数值点。不静默改用 CURRENT，也不自动发布删掉 restore 的 revision。`explain` 提示固定点覆盖 `resume_latest`；仅设周期但未开自动恢复的配置也有提示。普通失败并非无限重试（连续失败上限 16，稳定运行才重置）；单次 checkpoint 提交失败本身不等于整个 Job 重启。

提交内部使用不可伪造的 `EncodedSnapshot`/`commit_prepared` 避免把刚编码的状态再物化一遍；公共 `commit_encoded` 仍校验不可信字节。替换 CURRENT 前仍读取旧 chunk 并检查 CRC/codec，只是不构造整份旧状态；**PUBLISHED 只证明发布，不证明 chunk 未损坏**。已有有效发布证明不重写；没有删除的 prune 不额外 fsync。只读 inventory 使用 `open_readonly`，不创建目录/锁文件。

File checkpoint 会在阻塞工作线程上重新采样**实际已消费 cut** 的身份，修复“空文件启动后追加的数据没有纳入身份”的缺口；Unix 下拒绝路径已替换而旧 fd 仍活跃的提交。当前身份为有界首/中/尾抽样，不是全文件密码学校验，不能保证发现任意位置的恶意/违规原地修改。可信 append-only producer 合同仍是前提。

<a id="recovery-support-matrix"></a>
## 恢复支持矩阵（第11批起维护）

本节是恢复覆盖的权威矩阵；[DEVELOPMENT_ORDER](DEVELOPMENT_ORDER.md) 只引用它。状态含义：**已验证** = 实现并有匹配源码的独立 oracle/进程故障证据；**已实现** = 代码与单元测试在，进程级证据未齐；**暂不支持** = 启动/validate 前明确拒绝（`unsupported_restore`），只能 `restart_fresh`。未列出的组合一律视为暂不支持。

**窗口 / 聚合**

| 窗口 / 聚合 | 状态编码 | 外层版本 | 验证状态 |
|---|---|---|---|
| Count + COUNT/SUM/AVG/MIN/MAX | codec 1，tag 1..7 | File v3、JetStream v4 及明确开放的组合 profile | 已验证（既有批次） |
| ET 滚动、ET 跳跃 + 旧聚合 | codec 1，tag 1..7 | File v3 及明确开放的图 profile；不含 JetStream v4 | 已验证（既有批次） |
| PT 滚动 + 旧聚合 | codec 1 | paused v16/v17、图 v18 | 已验证（既有批次） |
| Count/ET 滚动/ET 跳跃 + FIRST/LAST/VAR_POP/VAR_SAMP/STDDEV_POP/STDDEV_SAMP（可与旧聚合混用） | codec 3，tag 1..9 | File v29 | 已验证（子批1：Kernel 级恢复等价单元测试 + 进程级 SIGKILL：Count、ET 滚动、ET 跳跃各 7 个可确认切点 ×20 轮，独立 oracle 逐位比对；证据二进制含测试专用 `process-fault-pause` 特性，非发行包字节；不含断电/介质故障） |
| Count + 新聚合 | codec 3 | JetStream v30 | 已验证（子批1：真实 NATS JetStream → Count → required HTTP，8 个可确认切点含 ACK 丢失 ×20 轮；校验值、顺序、OutputSequence ID、ACK 不越过 CURRENT；同上限定） |
| ET 窗口 + 新聚合 | — | JetStream | 暂不支持（子批2 单独验证） |
| PT 窗口 + 新聚合 | — | — | 暂不支持（子批2） |
| FIRST/LAST 输入为 nested/Dynamic | — | — | 暂不支持（validate/start 拒绝，与 MIN/MAX 同规则） |
| 滑动计数（单窗口，可含旧聚合与新聚合） | codec 4 `BWF1` | File v31、JetStream v32 | 已实现（子批2a：codec 黄金字节/逐前缀截断/结构不变量、恢复等价独立 oracle、Kernel 级 v31/v32 恢复含 OutputSequence 连续、owned 额度与低预算退款、不兼容不回退、Supervisor File v31 checkpoint→kill→恢复与 oracle 一致）；**已验证（子批2a 进程级 SIGKILL，冻结 `0b891be`（首轮 `02e1f99` 同样全过））**：File v31 与真实 nats-server JetStream v32 各 input_after/output_after/output_inflight/commit_before/manifest_renamed/commit_after/restore_kill + 空状态/窗口未满/恰在触发边界/低预算恢复退款（v32 另含 ack_lost）每项 20 轮全过，独立 ring-buffer oracle 逐位比对值、顺序、窗口区间、ID、重复/丢失、CURRENT/ACK 边界、资源归零；与 424cf95 及 #29 `adcbeae` 二进制兼容（旧二进制拒绝 v31、v29 目录由新二进制续写仍为 v29、size/step 变化拒绝） |
| ET 滑动/会话（单窗口，可含旧聚合与新聚合） | codec 4 `BWF1` kind 7/9 + 水位尾 | File v33 | 已验证（子批2b 进程级 SIGKILL：会话/ET 滑动各 input_after/output_after/output_inflight/commit_before/manifest_renamed/commit_after/restore_kill/empty/about_to_close/no_new_input/low_budget，会话另含 ooo_merged，每切点 ×20 于冻结提交 4810988，独立 oracle 全通过；与 424cf95/#29 main/#30 二进制兼容性通过。单元：codec 黄金字节/逐前缀截断/结构与派生范围不变量、每个切点恢复与不中断及独立 oracle 一致、Kernel 级 v33 恢复（空状态/将关闭/乱序刚合并/有输出后仍有开放状态/无新输入）、owned 额度与低预算退款、伪造记录与不兼容不回退）；JetStream + ET 暂不支持 |
| PT 跳跃/滑动/会话 | buffered | — | 暂不支持（子批2c，v34/v35） |
| Join / UNNEST | 有界分析状态 | — | 暂不支持（子批3） |
| IoT change/deadband/hysteresis/alarm/silence/resample | codec 2 | 各自 profile | 已验证（既有批次）；与新聚合组合暂不支持 |
| Dedup | — | — | 暂不支持 |

**时间域**

| 维度 | 范围 | 状态 |
|---|---|---|
| PT | TPD1/PTC1 逻辑钟（v14～v17）、GTD1/GTC1（v18） | 已验证（既有）；子批1 不扩时间协议 |
| ET | frame 内 wm_in/wm_out/last_effective；v29 原样复用；v33 在 BWF1 尾部存单输入 activity/wm/max_event_time/last_effective | 已验证（既有 v3）；v29 已验证（ET 滚动/跳跃 File 进程级 SIGKILL，子批1）；v33 已验证（ET 滑动/会话 File 进程级 SIGKILL ×20，子批2b）。已知限制：future-skew 用重放时墙钟（v3/v29/v33 相同）；线性 File 无 idle 生成器，ET 只在新事件/EOF 时关闭 |
| 图 ET idle/EOF | v19 | 已验证（既有）；与新聚合组合暂不支持 |
| 观测时间 | OFD1/OFC1（v23/v24） | 已验证（既有）；与新聚合组合暂不支持 |

**Source / Sink**

| 端点 | 恢复位置与确认 | 状态 |
|---|---|---|
| File / replay | 身份 kind/path/size/指纹 + offset/record_index；CURRENT 即切点；只承诺未提交后缀重放 | 已验证（既有）；v29 已验证（子批1 进程级 SIGKILL）；v31 已验证（子批2a 进程级 SIGKILL） |
| JetStream Source | BND1/JOW1 绑定 + consumer 序号 + OutputSequence；HTTP 2xx 且 CURRENT 落盘后才 ACK | 已验证（既有）；v30 已验证（子批1 真实 NATS 进程级 SIGKILL）；v32 已验证（子批2a 真实 NATS 进程级 SIGKILL，含 ack_lost） |
| MQTT / NATS Core / WS / TCP / HTTP Poll/Push / DataBus | 无可重放身份 | 暂不支持（restart_fresh；不因下游支持 checkpoint 获得重放） |
| Kafka / Redis / Postgres | 尚未通过自身恢复协议验收 | 暂不支持 |
| required HTTP JSON | v29/v31 无稳定 ID；v30/v32 带 OutputSequence | 已验证（既有 v3/v4）；v29/v30 已验证（含请求在途时 SIGKILL；子批1）；v31/v32 已验证（含请求在途时 SIGKILL；子批2a） |
| HTTP CSV（aligned） | — | 暂不支持 |
| JetStream Sink v27/v28 | 线性 File | 已验证（既有）；与新聚合组合暂不支持 |
| File/Action Sink 等其他 Sink | — | 暂不支持（子批5 outbox） |

**拓扑**

| 拓扑 | 状态 |
|---|---|
| 线性 ≤2 状态 | 已验证（既有）；v29 仅单 Count/ET 窗口或双 Count，v30 仅单/双 Count；双 Count 不以单窗口进程用例替代组合验收；v31/v32 只开放线性单状态，已验证（子批2a）|
| File DAG ≤16 状态 / ≤16 required HTTP Sink | 已验证（既有）；含新聚合暂不支持 |
| 双输入 / Join | 暂不支持（子批3） |
| 侧路、有损边、source-time、参考表/Lookup + 新聚合 | 暂不支持 |

**版本 / 依赖 / 恢复语义**

- 一个目录只允许一种外层 profile；SPV1 v1/v2 不迁移；File v3 保留 RCP2 下游前缀放宽，v29～v32 及 v8 之后的新 profile 严格匹配规范化语义与依赖身份（不是配置文本或 revision 号）。JetStream 即使旧 manifest 带有前缀标记，也要求完整语义不变；改语义直接拒绝，无 fork/migration（子批7）。
- 恢复语义：File 为未提交后缀重放（at-least-once，无稳定输出 ID）；JetStream 为稳定 ID 的 at-least-once，ACK 不越过 CURRENT。都不是 exactly-once；SIGKILL 证据不等于断电/介质故障认证。
- 恢复内存预留适用全部版本（见上节）；额度不足、profile/codec/语义不兼容均为不可回退错误。
- 子批1 进程证据切点：输入已入窗口未输出、输出已确认未提交、输出请求在途、CURRENT 发布失败、MANIFEST 已改名但 CURRENT 未更新（该代不被提升）、提交后、恢复中（已预留额度未物化）、JetStream 提交后 ACK 丢失。旧二进制（424cf95）拒绝启动 v29 目录且不改 CURRENT/不输出；新二进制可继续旧 v3 目录。未覆盖：断电/介质故障、JetStream + ET、PT 窗口、恢复中其他位置。
- 本批保持既有 JSON 浮点解析语义，不启用全局 `serde_json/float_roundtrip`。进程测试改用可精确表示的二进制分数输入（步长 `0.375`），仍逐位核对恢复状态和输出；这是隔离解析器差异，不代表已修复十进制解析精度。

**待单独修复：JSON 浮点解析精度与恢复兼容。** 原子批1 oracle 观察到默认解析与正确舍入可能差 1 ulp。2026-10-10 决定从 #29 撤出全局 feature 变更，不将数值正确性问题登记为普通性能优化。独立修复必须包含十进制/极值回归、旧 checkpoint 后缀续读的明确兼容或拒绝策略、File/JetStream 输入语义一致性，以及相同负载下解码吞吐/延迟对比；未验证前不能声称已解决。

## 升级、备份与回退

**核心增强 B1 的额外 catalog 门禁：** 新候选将 catalog schema 升为 **v3**，加入不可变参考表 revision 与历史 pipeline dependency。即使当前没有 Lookup，首次打开旧 catalog 也会事务升级；旧 v2 binary 将拒绝 v3 catalog。回退必须使用停机一致的升级前 catalog 备份，不能编辑 version 数字或删除 pin 表来绕过。表发布、固定绑定、GC 和验证边界见 [REFERENCE_TABLES.md](REFERENCE_TABLES.md)。Catalog 兼容拒绝与 checkpoint codec 拒绝是两个独立检查，不能把前者当作已经验证后者。

1. 保存配置 revision、capabilities、诊断与二进制/source manifest；先停 desired，再等待实际停止，关闭 Server，确认 writer lease 已释放。
2. 在服务停止时一起备份 catalog（含存在的 `-wal/-shm`）、原 File 数据、**整个 checkpoint 目录**和对应密钥；校验备份。不要只复制 CURRENT，也不要热复制 SQLite 文件当成一致性备份。
3. 在隔离目录用副本验证候选包启动、解密、validate/explain、指定恢复点和真实输出。只读列表不是备份可恢复验证。旧 snapshot codec 不就地升级，保留原件；不兼容时显式新 pipeline/fresh 或离线转换，不能编造恢复成功。
   **配置预检覆盖全部仍会使用的 revision**：包括当前 desired、数值恢复/人工固定版本以及预定回滚版本，不只检查 latest。用候选版本对这些 spec 执行 validate/explain；source inbox / sink outbox 必须在 `1..=4096`，HTTP outbox 另限 1024。历史超限配置也会被拒绝，不在启动时静默 clamp；升级前发布兼容配置并验证流量/背压。数值恢复点进程重启后的 held 需纳入值班操作清单。
4. 停服务后切换 `current` 到已校验的新目录再启动。确认实际 revision/attempt、输入计数与输出正确，保留旧包和升级前备份。
5. 回退必须停服务并切回旧包；若 catalog/checkpoint 新旧格式不兼容，使用**同一备份时点的整套副本**，不混用新 CURRENT 与旧 source。先在副本验证再重开真实 Sink。

配置/二进制回退不会撤销已发送 HTTP/MQTT；aligned 不等于 exactly-once，故障重放可能重复。没有持久 outbox，进程退出时尚未确认的输出不能被当作可靠提交。

## 故障处置

- `held`：先检查失败原因/目标可达性/凭据/身份，再显式 start 解锁；不要无限自动重试掩盖错误。
- 容量等待：检查 max-jobs 与队列/父 owner；不按增大队列来承诺不丢失。best-effort 过载丢失是失败样本/容量证据，不算无损通过。
- checkpoint 超时：先看 active 与最后提交点；文件系统提交不可强制取消。磁盘满/保留失败先停止输入与调度，检查配额和无关占用；不要删除 CURRENT 或仍需要恢复的代。修复后手动 checkpoint 并实际恢复验证。
- 不要在 checkpoint 目录手工创建 `chk-*`：非规范代名称或特殊文件会 fail-closed，错误包含相关路径。不要为“解锁”删除协作锁文件；真实锁占用为可重试容量错误，不支持锁等 OS 错误则单独报告为不可重试 Internal，先排查文件系统。
- 身份/codec/语义拒绝：核对源和备份，不编辑 snapshot 字节或跳过校验；明确选择合适版本/数据或显式 fresh。
- 诊断报告 registry_busy/unknown：重试采样，不解释为零占用/健康；没有成功年龄不等于刚成功。

## 本批资源合同

API 并发请求 128、阻塞 API 工作 64、catalog/冷启动工作 128。`API_REQUESTS` 在响应/504 返回后退款；`API_WORK` 跟随真实阻塞闭包，不因等待者超时而提前退款，二者不是同一 permit。source inbox/outbox 受配置上限，HTTP 另有更严 1024 outbox/4 MiB 聚合缓冲约束。File 64 KiB 记录/32 条读取批次，HTTP 编码在增长前获取信用，response drain/timeout 有界；超大输出明确失败，不静默拆分。

checkpoint 单份 8 MiB、MANIFEST 256 KiB、最多 4096 chunks；读取前和读取过程中均限长，目录扫描/递归有界，拒绝符号链接/特殊文件。checkpoint chunks 借用已编码切片，不再复制整份 snapshot。控制元数据与第三方 TLS/HTTP/SQLite/系统分配不宣称全部纳入 payload owner；全进程 RSS 硬上限、完整 Scalar arena 仍不是当前合同。

当前 Kernel 的 Filter/Project/Map eager scratch、Window/Dedup 暂存与关闭输出在生成之前先取得保守 Reservation，并保留到有 owner 的输出 batch 接手。StateKey 计入实际容器容量/嵌套值，MIN/MAX 使用先准入、后克隆候选的更新，额度/表达式失败保留原值；payload 先销毁再退 lease。保守额度可能更早拒绝低内存/大表达式配置，不能把它当作精确 RSS。公共 raw Row/Scalar/Vec 返回型 helper 与第三方解析器是单独的有限输入边界，不承诺任意库调用都自动计费；长期 arena 仍未实现。

R10 修正 scratch 公式：Window/Dedup 不再每批预留两倍全部 retention，保留量/关闭索引使用 O(1) 计数；本批输入与真正到期的有界输出分别准入。raw 输出转成 owning batch 后、异步发送前释放 scratch，不能在输出仍未接管时提前退款。MIN/MAX 复用候选容器，值未变化不 detach；只有增长才扩 lease，替换成功后按实际大小退款。融合表达式分别估计共享值大小与新分配，不再按列数/融合次数乘算整行。快照 wire 估算与 resident 估算分离，排序引用数组另行短期计费。

`max_state_keys` 是数量上限，不是任意宽度状态一定装得下的承诺。Server 每 Job 为共享的 4 MiB reservation / 4 MiB retention，key 上限为每 operator 1024；K1 双状态允许两者各 1024，但不各给一份 Job 字节预算。单 operator 4096 keys 并非默认支持档位。总预算、输入宽度、累加器/索引和并发输出仍共同约束准入，不能靠静默扩大测试预算宣称通过。

`/metrics.state_keys/state_bytes` 是最近一次 Window 采样，不是所有 Job 的总和；key 数现在取实际状态项，不再错误地使用 MemoryLease handle 数。`state_sample_scope` 明确这一限制。

R10 去掉了按 stage 深拷贝整个恢复快照的问题；K1 接手完整实例集合，在同一 owner 下准备各 Window，按实例重建后立即释放其 decoded handoff，阶段上下文只共享已准备的实例容器。重建按逐项最大 key/accumulator 暂存及有界 bookkeeping 预留，不额外预留三份完整快照。scratch/retention/index 准入失败则清理全部准备结果，不激活输入；仍不承诺任意快照或任意预算都可恢复，也不把 raw helper 宣称为事务式原地更新。

## 自审前 v7 验证记录（2026-09-14）

> 以下为历史证据；最新 K1 结果见 [K1 实现与验收](#k1-validation)，R10 单独保留。

以下数字和指纹仅对应自审前 v7，不是后续修复代码的新证据；自审后的复验单独记录，不覆盖原始失败或性能样本。

平台：`box@100.64.0.16`，Linux 6.12.94+ x86_64，rustc/cargo 1.98.0；本机未执行 Cargo 编译。原始记录：`/workspace/bench-compare/production-artifacts-20260914/v7/`。

| 验证 | 实际结果 |
|---|---|
| 核心 / 独立无 demo Server / CLI | 490/0、25/0、2/0；各 profile 单独执行 |
| 固定二进制重复 | 79 项 × 20 轮，1,580/0；10 个测试二进制 |
| 真实进程恢复 | 指定点 sum=60；kill -9 后显式自动恢复 sum=150；实际备份文件回读 sum=240；无多余输出 |
| 生命周期 | 20 次启停 fd 12→12，SIGTERM 退出与锁释放；配置不兼容恢复不改 CURRENT |
| 鉴权/危险操作 | demo、重复 catalog、错误 token、远端明文、覆盖诊断文件、不兼容恢复等负向用例通过；TLS/超时/响应体等连接器回归包含于核心测试 |
| File 80k ABBA 对 R9 | 554,601→563,685 rows/s，中位比 1.0164；RSS +440 KiB |
| File 400k ABBA 对 R9 | 584,175→577,289 rows/s，中位比 0.9882；RSS +392 KiB |
| 周期 100 ms 开/关匹配 ABBA | 9,258→9,221 rows/s，比 0.9961；开启的 4 个正式试次成功 13/13/13/14 次，失败均 0；RSS 差 -24 KiB 属采样噪声 |
| MQTT 2k 输入/s | 7,500/7,500 输出，0 缺失/重复；capture-arrival p99 11.178 ms；466 POST / 3 TCP |
| MQTT 120 s、1k 输入/s | 90,000/90,000 输出，0 缺失/重复；capture-arrival p99 6.700 ms；18,934 POST / 1 TCP |
| 刻意过载 | 1,412/1,536 输出，124 缺失、0 重复；无损判据下 **invalid**，不伪装成通过 |
| 观察守恒 | 267 个视图 / 534 个边界队列 / 547 个 Runtime mailbox 样本一致，计量错误为 0 |
| 网络/长稳 | netem qdisc 不可用，**NOT_RUN**；真实 WAN、目标设备、24/72 h、10,000 次循环未运行 |

File 共 20 个正式试次 + 8 个缩小输入 warmup，均 valid；同规模输出 hash 一致。周期开/关 8 个正式试次 + 4 个 warmup 均 valid。预先登记门槛：File ≥0.97、RSS 增量≤1,024 KiB；周期开/关≥0.90、RSS 增量≤2,048 KiB，最终均满足。当前结果不表示所有场景都更快；p99 是捕获端到达时间，不是远端业务确认或 WAN RTT。

v5/v6 的 400k 分别约 3.1%/3.3% 开销，未过预设门槛，原始样本保留。之后去掉固定宽度聚合的多余查表/重计费、修复 group key 被 detach 两次的分配浪费；保留 scratch/owner/恢复安全校验，没有改阈值或隐藏失败。历史 v1/v2 的编译/鉴权兼容问题与打包依赖检查误匹配 `sparrow` 为 `arrow` 也保留在独立日志中。

v7 代码证据（不包含根目录过程 MD）：

- 基线 `4f70407`；代码 patch SHA256：`f0eec40539e717436998d0b3d4701579689de4b4bad3e57d876e02e9f7a27d2e`。
- 231 个构建/源码/fixture/脚本文件清单 SHA256：`4cdf5a9379b6888342f6db1ab39eb4ece528d04517586b603718c826f066dcd4`；干净基线重放与服务器逐文件校验通过。
- Server SHA256：`d0b13b92248e38dbcda8478f74c9bf497f705dab48fbc2d0a22cdc225786a13e`。
- CLI SHA256：`4584cf502baf8edbfe3096baebade9ab7df2c5c42d7fc0b49b95ce17204798d4`。
- R9 固定 driver：`4d5ffd198c7ace2183237c30a0a4790127b45b52fca5d2f2b3a5a9f81af38c7a`；周期 driver：`1385f3de40af74ef2038e40257c57f7d03bd9b3f5d248d27d3c9b2828cf030a8`（v5 冻结，后续 Window 优化没有改 driver/采集协议）。

复用脚本：`production-build.sh`、`production-smoke.sh`、`production-repeat.sh`、`production-performance.sh`、`obs-closure-verify.sh`、`obs-check-snapshots.jq`。既有 Mosquitto PID 2657777 未被停止，host qdisc 未修改。systemd 文件仅为部署模板；实际安装/云端 CI/commit/push/tag 都没有执行。本轮可进入集中 review/候选冻结，不等于正式生产认证。

<a id="self-review-20260914"></a>
## 自审修复与复验（2026-09-14）

本节覆盖 v7 后的五项修复，原始证据独立保存在 `/workspace/bench-compare/production-self-review-20260914/`：

1. **生命周期取消安全**：旧启停 future 被取消会释放 transition 锁并脱离尚未结束的子任务。`before.log` 保留新增回归在修前的实际失败；现在锁/子任务跟随有界独立工作，kill、stop-all 与 converge 使用同一入口。
2. **restore 原子发布**：原来的配置写入→kill→desired start 存在取消/故障窗口。现在新 revision 与 desired 在同一事务提交；注入 COMMIT 失败时 revision、desired 和失败 hold 一并回滚。API 用例覆盖运行中旧版本到所选恢复点的替换。
3. **恢复预算过度预留**：原实现 decoded snapshot 计费后再预留三份完整副本。新回归构造同一 64 KiB 预算下可运行、可编码的状态，确认旧预留公式失败，而逐项重建通过；保留 decoded ownership、retention/index 和临时分配准入。
4. **flush deadline 与取消**：移除 sink 写死的 5 秒。虚拟时钟覆盖 8 秒策略下第 6 秒 ACK、100 ms 截止以及已放弃 barrier 立即解除等待；pending 与实际丢失不被取消动作清零。
5. **构建目标与来源**：显式 target 同时用于 build/tree/复制路径。实际在继承 `CARGO_BUILD_TARGET=aarch64-unknown-linux-gnu` 的环境构建 Linux x86_64 包，并执行版本检查、包校验和真实进程 smoke；相对 source-manifest 校验通过。

嵌入调用兼容说明：`Supervisor::converge_once/kill_named/stop_all/shutdown` 的 receiver 现为 `&Arc<Self>`，使用 `Supervisor::new` 返回的 Arc 调用；不能再仅持裸 `&Supervisor` 调用这些生命周期方法。HTTP 路由不变；取消调用者不再撤销已准入操作。

已复验：核心 **495/0**、独立 no-demo Server **26/0**、CLI **2/0**；固定 **11** 个测试二进制的关键 **87 项 × 20 轮 = 1,740/0**，包含跨 profile 重叠，不视作不同的独立用例。真实进程再次验证 sum=60/150/240、实际备份回读、20 次启停 fd **12→12**、SIGTERM 与锁释放，以及原有六类负向用例。测试/打包在服务器完成，本机未执行 Cargo 编译。

性能也使用本次实际生产二进制复测，仍沿用原脚本和预先登记门槛，未重新编译 driver：

| 本次比较 | 吞吐中位比 | RSS 差值 | 正确性/门槛 |
|---|---:|---:|---|
| File 80k，自审后 / v7 | 0.9974（551,845→550,385 rows/s） | +172 KiB | 通过 |
| File 400k，自审后 / v7 | 1.0212（572,753→584,898 rows/s） | -104 KiB | 通过 |
| 当前候选周期 100 ms，开启 / 关闭 | 0.9825（9,258→9,096 rows/s） | -196 KiB | 通过；开启试次提交 14/14/13/13，失败均 0 |

File 20 个正式试次 + 8 个 warmup、周期开关 8 个正式试次 + 4 个 warmup 全部 valid；同规模正式输出 hash 一致。File 门槛仍为 ≥0.97、RSS 增量≤1,024 KiB；周期门槛仍为 ≥0.90、RSS 增量≤2,048 KiB。这里 File 的分母是 **v7**，不是 R9；小幅增减不解释为所有场景都会提速，负 RSS 差值也不作为确定的节省承诺。

源码/产物指纹：

- 基线 `4f70407` + patch：`8f6d35f3f831cce88466efa59d33648e2c538470a39c669f45903c40c36e8972`。
- 排序的 231 文件清单：`577c064675368e7193ee5241aa974a0ab73a5b412e9ce9b1fe987264bd76b224`；干净基线重放和服务器逐文件校验通过。此清单与 build 脚本清单的排序不同，不能直接比较两个清单文件的 SHA。
- Server：`ce5e6112f32a56bd361780dd6fe2a7540d7eb7e3f960c6afe0e5f8f932845c61`。
- CLI：`d15573b920a48bd1360bc61e9944effc632a7809b396e83a2d564707fb8ec96a`。

本次未重跑 MQTT 120 s、真实 WAN、目标设备或 24/72 h 长稳；前节 MQTT 数据仍只对应 v7。没有 commit/push/tag/安装服务，阶段 3 发行门禁保持未放行。

<a id="r10-validation"></a>
## R10 修复与复验（2026-09-14）

范围是 review 指出的现有路径缺陷及其验证，不是下一阶段功能开发。全仓纯格式变化已撤回：65 个文件逐一确认只等于“修前源码经 rustfmt”才恢复，保留之前未提交的功能改动。全仓格式化/阻断式 fmt 门禁留作独立清理；没有按 review 的示例直接拆 commit，因为当前控制面依赖运行时新接口，须按依赖顺序另行验证可 bisect 性。

**修复对应关系**：§1 的 Window/Dedup scratch、O(1) 状态计费、输出接管再退款、MIN/MAX 复用/实际退款、融合表达式 bound、freeze wire/workspace 分离已落地；§2.1 用固定点失败/重启 held 代替自动旧点重放；§2.2 去掉重复物化/proof 重写/无删除 fsync，保留旧 chunk 完整性验证；§2.3 拒绝弱/占位 token；§2.4 先 draining 后 join；§2.5 补历史 revision 升级预检，不静默 clamp。其余锁、只读 inventory、arity/函数注册、CLI 错误、诊断窗口、脚本/CI/部署模板与运维说明同步处理。

`max_state_keys` 的旧“2×全部 retention”校验建议不再适用，不能把过时公式放进 validate；当前数量上限与动态内存上限的区别见资源合同。review 建议的“有 PUBLISHED 就跳过 chunk 验证”没有采用，因为它可能在 CURRENT 损坏且新提交失败时回收最后有效回退点。单独故障注入回归证明该保护仍然成立。

服务器证据目录：`/workspace/bench-compare/r10-artifacts-20260914/`，源码目录：`/workspace/bench-compare/r10-source-20260914/`。仍是 Linux x86_64 / Rust 1.98.0；本机没有执行 Cargo 编译。`before.log` 保留三个新增内存回归在**修前生产化候选**上的实际失败，分别触发全状态 scratch、MIN/MAX 幻影计费、融合投影乘算问题；修后通过。最初编译错误和测试 fixture 修正记录保留于 `core-first.log`、`core-second.log`、`validation-1/2/`，不覆盖失败记录，不把加大 Job 预算当修复。

| 验证 | 本轮结果 |
|---|---|
| 默认核心 profile / 独立无 demo profile | **513/0 / 30/0**；后者为 Server 28 + CLI 2，包含独立 feature 编译 |
| 固定二进制重复 | **107 × 20 = 2,140/0**，12 个二进制；跨 profile 重叠不视为独立用例 |
| 多 key 实际 Kernel | 1024 keys、MIN + SUM、宽输入；Count / PT / ET + 慢输出，输出唯一且精确，结束后 task/physical 清零；另含 MIN/MAX 无关列、宽融合投影、父子退款、状态计数缓存回归 |
| snapshot / 控制面 | 有界校验与 decode 对照、chunk 损坏+发布失败保留回退点、旧 proof 不重写、只读无创建、固定点非 safe-mode 失败 held、停机拒绝新变更 |
| 真实进程 smoke | 固定点重启 held → 显式 start 原点；sum=60/150/240、kill -9 自动 latest 恢复、实际备份回读；20 次启停 fd **12→12**；SIGTERM 与锁释放、七类负向用例通过 |
| MQTT 120 s 短程复验 | 1,000 输入/s，120,000 输入、**90,000/90,000** 预期过滤输出，缺失/重复/错误均 0；另有 12 s warmup（9,000/9,000），不是 24/72 h 长稳 |
| 构建/静态检查 | 两 profile 冻结、clippy 成功（保留 warning 日志）、脚本语法、包 SHA256 校验；缺 ss/脏树正式包/非法构建模式均拒绝且不创建目标目录。云端 CI 和 systemd 实际安装未执行 |

性能预先登记并保留全部试次：多 key 用新固定 driver（Sparrow-only）；小 key 沿用历史固定 driver，不重编译/替换采集逻辑。**三组 File 的分母均为同工具链新构建的干净 `4f70407`，不是 v7/自审候选或 eKuiper。**

| 形状 | 基线 → 本轮（输入 rows/s 中位数） | 比值 / RSS 差值 | 结论 |
|---|---:|---:|---|
| 1024 interleaved keys，MIN/MAX/SUM，131072 rows，window=128 | 291,300 → 297,151 | **1.0201 / +532 KiB** | 12 正式 + 4 warmup，全 valid，同一输出 hash |
| 小 key File 80k，window=800 | 550,378 → 560,328 | **1.0181 / −108 KiB** | 8 正式 + 4 warmup，全 valid |
| 小 key File 400k，window=800 | 584,098 → 567,886 | **0.9722 / +276 KiB** | 12 正式 + 4 warmup，全 valid；约 2.78% 开销，接近但未越过 3% 门槛 |
| 当前版本周期 checkpoint 100 ms，开启 / 关闭 | 9,278 → 8,902 | **0.9594 / +316 KiB** | 8 正式 + 4 warmup，全 valid；开启提交 14/14/13/14，失败均 0 |

门槛未调整：File/多 key 吞吐 ≥0.97，RSS 增量分别 ≤1024/2048 KiB；周期开关 ≥0.90、RSS 增量 ≤2048 KiB。只说明这些有限形状通过，不宣称普遍提速，也不从单机 capture-arrival 推断 WAN p99。多 key 采用真实 Server 的 1024-key 上限，没有偷偷改成支持 4096 keys；warmup 至少一个完整 keyed window 周期，因此该形状 warmup 也是 131072 rows。

源码/二进制证据（不含根目录过程 MD）：

- 干净 `4f70407` + 完整源码 patch：`92a5a0eeca5bd3e13ee1bbd826600d7a9d54ba2bf612d725314e9954a5be7624`。
- **233** 个构建/源码/fixture/脚本文件清单：`04098c538de938551305d1c9239051185ef0f244739f273fa5c42c8455051e29`；干净目录重放、本地/服务器逐文件核验通过，与包内源码清单零差异。
- Server：`5b0601051f193c3fcfcf2dff8542e148bc835409cee60f1004256f166132fcbe`。
- CLI：`ad06c0f9a71e219024799c9742995c0ff30257140a0e4d9d5571a5880433e55f`。
- 多 key driver：`a8129b5eeec8eb2b51ed1c89cc16be2f750ab22f2f5f6e8efb485a0ef97af734`；干净 HEAD Server：`82399108c9f4f06f2429bc7618d50a5fa8d6b8e02be568feac9befe6bb0ac385`。

目标设备、真实 WAN/netem、24/72 h 长稳、掉电/flash 失败模型仍未放行；没有 commit/push/tag/安装服务。R10 的有限范围修复验证不等于整个 MEM arena、K1 恢复协议或正式生产认证完成。

<a id="k1-validation"></a>
## K1 实现与验收（2026-09-14）

> 本节保留 R11 修复前 K1 v8 的历史数据与当时判断。RCP2、source 并行推进及增强 smoke 属于 R11，不能用本节旧数字为其背书；当前实现合同以上文为准。

基线为已提交/推送的 **`9a92527`**，功能位于独立分支 `feat/k1-checkpoint-participants`，尚未提交或推送。不是修改 R10 的历史证据，也不更改版本号/tag。实现范围、v3 迁移规则与有限文件 EOF 行为见本页当前运行合同。

**功能批次已闭合，性能按场景分别放行，不是所有门禁全绿。** 真实 Source → 零/单/双状态 → required Sink → 持久化 → 新进程恢复已贯通；没有新增 broker、outbox、DAG、UI 或 Arrow/JIT。Source/Sink 配置参数可调整不等于全计算语义都可复用，恢复仍要求匹配实例集合和 CP01 描述。

| 验证 | 本轮结果 |
|---|---|
| 默认核心 / 独立无 demo | **525/0 / 32/0**（后者 Server 30 + CLI 2）；各 feature profile 单独测试并冻结 |
| 固定二进制重复 | **120 × 20 = 2,400/0**；跨 profile 重复不视作独立用例 |
| 实际 Kernel 切点矩阵 | 零/单/双 Count，空输入/全过滤/部分/完全闭合切点；原始输入独立求和校验；单 ET tumble/hop 的水位/状态恢复 |
| 配额与故障 | 双实例各 **1024 keys** 编码/恢复通过，共享字节预算不倍增；部分 freeze 配额失败后可重试；慢 Sink 真确认、取消释放；缺失状态在输入前拒绝；坏 codec/未发布代不提交或不恢复 |
| 实际 API/进程 | 零状态/双 Count 的手动、周期、选点恢复；kill -9 后分别输出 **101 / 21**，各仅一条；generation 延续；generation 持久化失败时 `jobs_started=0`、`ingested_rows=0` |
| 旧格式 | 真实 R10→K1、K1→R10 都拒绝不兼容恢复，CURRENT 不变、无输出；不是自动迁移成功 |
| 原操作闭环 | sum=60/150/240、真实备份回读、固定点 held、20 次启停 fd **12→12**、SIGTERM/锁释放及原七类负向用例通过 |
| checkpoint API 延迟 | 零/双状态各 **240** 个正式样本；p50 **7.778 / 7.349 ms**，p99 **15.024 / 13.569 ms**，max **17.419 / 14.504 ms**；含对齐、Sink 和磁盘，不是逐行延迟/硬暂停上界 |
| 恢复到 Running | 零/双状态 **63 / 62 ms**，仅各一次真实进程样本，含 CLI 轮询/启动，不当作恢复 p99 或大状态保证 |
| MQTT 回归 | 2k 输入/s，10,000 输入 → **7,500/7,500** 输出，无缺失/重复；capture-arrival p99 **11.320 ms**；467 POST / 3 TCP；观察 66 个视图、132 个边界队列、132 个 mailbox 样本守恒 |
| 静态/构建 | `git diff --check`、脚本语法、独立 production build/包 SHA256 与 clippy 通过（保留 warnings）；本机未运行 Cargo 编译，云端 CI 未执行 |

### 性能：保留 100 ms 压力点失败，不靠改门槛过关

所有正式与 warmup 试次均按真实输出校验，同输入规模的正式 hash 一致；以下比值使用各组中位数。fresh 路径与 R10 固定二进制比较；周期 on/off **都运行 K1**，不把旧版无法运行的 aligned 零/双状态当作基线。普通路径目标 ≥0.97，周期目标 ≥0.90；RSS 阈值沿用各脚本预登记值，未降低。

| 形状 | 吞吐比 | RSS 差 | 结论 |
|---|---:|---:|---|
| 既有单窗口 File 80k / 400k，K1 / R10 | **0.9946 / 0.9962** | +52 / +36 KiB | 原 3% 门槛通过 |
| 零状态 fresh 32k，K1 / R10 | **0.9816** | −4 KiB | 通过；不是内存节省证明 |
| 双 Count fresh 131072，K1 / R10 | **1.0055** | +328 KiB | 通过，小幅差异不夸大 |
| 单窗口周期 100 ms，on/off | **1.0061** | +584 KiB | 通过；4 次正式 on 试次各成功提交 13 次、失败 0 |
| 双 Count 周期 100 ms，on/off | **0.9732** | +148 KiB | 通过；提交 10/12/12/12，失败 0 |
| **零状态 + 20 ms Sink + 100 ms 周期**，6400 输入 | **0.8443** | +144 KiB | **性能门禁未过**；正确性通过，不伪装为全绿 |
| 同一零状态压力配置，扩大至 25600 输入、100 ms | **0.8625** | −172 KiB | **仍未过**；提交 71/66/67/66，失败 0，保留复测反例 |
| 同一 25600 输入/慢 Sink，周期改为 **500 ms** | **1.0088** | +320 KiB | 该配置通过；4 次各提交 11 次、失败 0 |

按当前实现，aligned 切点需要等待 required Sink 完成并同步提交；高频切点会中断持续输出的流水执行。频率对照支持这是该负载下的重要开销来源，但未将 flush、文件轮询相位和 fsync 各自耗时完全分离。**500 ms 的通过不是 100 ms 已被代码优化**：本批保留安全切点语义，不通过跳过 Sink 确认、只在空闲时伪造周期成功或修改阈值提分。此慢 Sink/零状态组合不按 100 ms 性能目标放行；500 ms 是本机已测配置，模板 10 s 及真实目标环境仍需按 RPO/负载验收。并行前缀 ACK/持久输出协议留在后续可靠链路工作，不在 K1 偷换保证。

### 复现与来源

原始目录：`/workspace/bench-compare/k1-artifacts-20260914/`；源码：`/workspace/bench-compare/k1-source-20260914/`。最终用 `validation-v8/`、`package-v8/`、`smoke-v8/`、`k1-smoke-v8c/`、`regression-v8/`、`performance-v8/`、`performance-v8-two/`、`zero-interval-100/500/`、`manual-states-0/2/` 与 `mqtt-regression-v8/`，不覆盖旧版本数据。

```sh
bash scripts/production-k1-smoke.sh PACKAGE NEW_EVIDENCE_DIR OPTIONAL_R10_SERVER
bash scripts/production-k1-performance.sh NEW_EVIDENCE_DIR FROZEN_DRIVER R10_SERVER K1_SERVER
# 同输入规模，单独验证不同频率；默认 100 ms 的失败样本仍保留。
K1_PERF_STATES=0 K1_PERF_MODES=periodic bash scripts/production-k1-performance.sh NEW_DIR DRIVER R10 K1 500 25600
```

保留开发失败记录：旧测试把零状态当作错误的预期已换成新的正向验证/仍不支持 PT 的负向验证；保持 sealed EOF 完成的回归未放宽。`package-v5/v6` 曾命中共享 **显式 x86 target** 的旧依赖，虽然源码存在 `semantics`，旧 rlib 没有该模块；只清默认 target 不够。显式 target 的工作区包缓存清理后重建成功，第三方缓存保留。归档保留 mtime 时不能只信 Cargo 的 Fresh 提示；建议独立 checkout/target，并校验源码清单和实际二进制。smoke 首次端口碰撞、脚本误用不存在的 CLI metrics 命令也保留，随后固定非 ephemeral 端口、调用已存在 API 复验。手动延迟计划误写 chunk=800，实际 driver `CHUNK=400`；修正单独记录，240 个样本没有改动或删减。

- 源码基线 **9a92527** + 完整 patch SHA256：`abe92f9edd95b4dd99757d40439f542a858bf039e5e17e6ce06d4a4bdb64a641`。
- **240** 文件清单 SHA256：`70db97c096053b74800c0d7046850dbec5efa17c439085ef3f698608ad58dc32`；干净基线重放并逐文件校验。
- Server：`e989e1903bf7571de50d26617d3efb765843337d249163b9961c6fc3ba7dca77`。
- CLI：`5bc2ce61be23de013287d786acd2e92880eb8202d7a0a13bb420717e2f29115b`。
- K1 driver：`34330536f714268ed7482d4282041604fb9db5ec8744a54581aeabdba622f2f0`。

本节不等于正式发行或全配置性能放行。100 ms 零状态慢 Sink 压力点、真实 WAN/netem、目标设备、24/72 h 长稳和掉电介质验收仍未放行；未 commit/push/tag/部署。K1 可以进入集中 review，下一功能模块仍为 K2 可靠输入/输出。

<a id="r11-validation"></a>
## R11 收尾与独立自查（2026-09-15）

最终受测候选为 `package-v8`，源码基线 `9a92527` 加 K1/R11 补丁；以下不覆盖任意网络、设备和断电场景。原始目录 `/workspace/bench-compare/r11-artifacts-20260915/`，源码 `/workspace/bench-compare/r11-source-20260915/`。本机未 Cargo 编译，服务器分组测试、冻结二进制后重复执行；完整入口 `sparrow-r11-final-validation.sh`，`final-v8.exit=0`。v8 相对 v7 最后校正了 API 的 plain CP01 兼容提示和对应断言，仍重新执行全部最终门槛，不将 v7 数字冒充最终版本。

### 修复与自查闭环

- CI 事件 diff base 无效时回退 first parent，无 parent 则明确 warning/跳过，不再空树扫描；历史行尾空白修复。CI 单独 worktree/target 构建固定 R10 基线，旧 codec smoke 成为必跑项；缺基线为 PARTIAL、退出 4。没有把本机/服务器脚本执行说成 GitHub 云端 CI 已运行。
- File 在按序捕获 cut、注入 barrier 后继续读取，独立单飞 worker 收齐 ACK 后提交；取消/EOF/读错误等待 worker 收尾，不能 detach 正在执行的 blocking commit。多 chunk 的目录同步合并为发布 ACK/MANIFEST 前一次，文件同步及 CURRENT/PUBLISHED 顺序不变。
- **性能反例与根因：**只做 source 并行的 v5，零状态 100 ms 仍为 0.8413，25600 输入为 0.8466。发现 HTTP `force_flush` 让每个 Runtime batch 都变成小 POST，同一 6400 试次请求数 112→157；独立测试修前为 18 个单行 POST，修后为 `8+8+2` 三个 POST。现在合并已排队前缀，只让尾部跳过 linger，并按请求时 sent 计数结束强制状态；不改变 required Sink ACK 条件。
- **自查发现的回滚反例：**中间候选 CPL2 外层令旧 K1 在混合历史中静默回退到 checkpoint 1，重复输出 `6,6`，真实进程记录在 `k1-rollback-before/`。最终改为旧 Store 可完整解析的 CPL1 外层和 CP01-compatible RCP2 语义封套，在兼容检查明确拒绝。`k1-smoke-v8/` 验证 plain CP01 升级可恢复，生成新点后旧 K1 回滚拒绝、CURRENT 不变、只有一条 `6`；不是删旧代绕过问题。
- 状态依赖前缀允许最后状态之后的计算更新，旧 plain CP01 保持全计算严格匹配；窗口之间的变更拒绝。R10 v1/v2 目录在 fresh/restore 两条 Server 路径都拒绝混写。所有准备窗口缺失拒绝、去掉深拷贝 handoff 的 Clone、共用 FreezeHeader、按实例配额、freeze workspace 与精确 metadata Vec 容量均已处理。
- 补齐 key/accumulator/COUNT/ET 边界、未知/重复实例、同值/冲突 State ACK、owner 隔离、generation 初始化/写失败、64 实例、legacy reader 与多 chunk 诊断测试。失败用消息区分而非仅错误码/超时，freeze 失败当场检查退款。held 保留原错误，快照诊断展示版本/revision/attempt/generation。
- 辅助 retention/index 计数 release 下检查加减，错误计入 `state_accounting_errors_total` 并使 stage 失败，不静默 wrap/归零；锁采用 exclusive create、已有路径不带 create 重开，避免替换为 dangling symlink 后在目标处创建文件。旧 CRC/GC 验证不因缓存诊断头而省略。

### 最终匹配验收

| 检查 | package-v8 结果 |
|---|---|
| 核心 / 独立无 demo | **540 / 34** 通过；后者 Server 32 + CLI 2，`validation-v8/` |
| 重复与静态 | **140 × 20 = 2800** 通过；clippy 退出 0，历史 warnings 保留；脚本语法、diff check、CI 四种 base 输入通过 |
| 生产 smoke | 输出 60/150/240，20 次启停 FD **12→12**，7 项负向、固定点 held、SIGTERM join、实际备份恢复通过 |
| 强化 K1 smoke | kill 前已有输出，零状态总输出 **[200,101]**，双 Count **[21,57]**，不是 fresh 重放也能通过的 oracle；两个进程恢复观测均 63 ms（单次、含 CLI 轮询，非 p99） |
| 升级 / 回滚 | R10 双向 codec 拒绝、旧目录 fresh 拒绝且不激活 Job；历史 K1 升级/回滚反例修后通过。CI 的可复现基线为 R10，未提交 K1 历史包的进程测试是服务器附加项，Rust 另覆盖旧 reader 外层语法 |
| 手动 checkpoint | 零/双状态各 32000 输入 × 3 轮，CHUNK=400，各 **240** 个测量；p50/p99 分别 **7.411/14.596 ms**、**7.466/14.277 ms**，含对齐、Sink、API/磁盘，不是逐行延迟或硬停顿上界 |
| MQTT 回归 | 10000 输入、2000/s，过滤后 **7500/7500**，无缺失/重复/错误；845 POST、1 TCP，capture 到达 p99 **6429 μs**。仅短回归，不代替 keepalive/长稳认证 |

性能保持原阈值：fresh **≥0.97**、周期 on/off **≥0.90**，RSS 增量 **≤2048 KiB**；所有 warmup/测量输出正确且各匹配负载 hash 相同，无丢失/重复。最终候选预先登记三组 fresh ABBA，全部 36 条测量样本合并（每 variant 18 条），未删掉任意一次试次。

| 负载 / 比较 | 吞吐比 | RSS 增量 KiB | 结论 |
|---|---:|---:|---|
| 零状态 fresh 32000，R11/R10，三组 ABBA | **0.9870** | 224 | 通过 |
| 双 Count fresh 131072，R11/R10，三组 ABBA | **0.9928** | 232 | 通过 |
| 零状态 6400 / 20 ms Sink / 100 ms，R11 on/off | **0.9994** | 124 | 通过，强制 flush 不再拆碎排队前缀 |
| 双 Count 6400 / 20 ms Sink / 100 ms，R11 on/off | **0.9475** | 428 | 通过，有约 5.3% 开销，不宣称所有形状零成本 |
| 零状态 25600 / 20 ms Sink / 100 ms，R11 on/off | **1.0059** | 256 | 通过 |

周期 checkpoint 每个测量试次均实际成功且失败数为 0：零状态小规模 18/轮、双 Count 12/轮、零状态大规模 58/59/59/58。`performance-v8`、`fresh-v8-1/2`、`zero-100-v8` 均退出 0。v6 的首个 fresh 零状态比值 0.96938 曾略低于门槛，保留原失败及预先登记的两组复查；最终 v8 独立登记/执行完整三组，不把 v6/v7 结果混入最终汇总。

来源与复现：driver 为冻结 K1 v8（SHA256 `34330536f714268ed7482d4282041604fb9db5ec8744a54581aeabdba622f2f0`），R10 对照为已核对源码的固定生产包，均未在共享 target 重编译。脚本 `production-k1-performance.sh` 默认跑 fresh+periodic；额外 fresh 用 `K1_PERF_MODES=fresh`，大规模用 `K1_PERF_STATES=0 K1_PERF_MODES=periodic ... 100 25600`。原始计划、每组 summary、日志和 `.exit` 都在证据目录。

- 完整代码/脚本补丁 SHA256：`f2242a1a25004a4ddc5f6ade6b532a76d9d6a3b6c58dbad34bbf0ffc3b426df1`，从干净 `9a92527` 重放，**241 文件**逐一校验。
- 源码清单 SHA256：`e2501d41e4940f40cd9ea6015c28764ca67d48bc2d6cf0d2464624fc40777bc4`，与生产包 source manifest 相同。
- Server SHA256：`6feffdd99734885bf3777a5c3fd211bf5186284fbefb0ca594fc2b49d7d2abde`；CLI：`9d7df7ca6ce43988f55096a52fa75f7b7fc700046bdaeefa72b9aaa7c7ea2a19`。

保留开发过程反例：CPL1/CPL2 查找 fixture、API If-Match/allowlist/busy polling、直接 Window 测试需消费 pending emission 才推进 holdback，均在 `core-v1..v4` 与 direct API 日志中；没有改预算或放宽原断言来制造通过。R11 的 MIN/MAX 批内 key 缓存、上一代 CRC/GC IO 缓存和 held catalog 读缓存仍为后续低优先级优化，不冒充本轮已实现。真实 WAN/netem、目标设备、24/72 h 长稳和掉电仍未认证；本批不自动 push/tag/部署。

<a id="k1-k4-reference-validation"></a>
## 2026-09-17：K1～K4 静态表组合恢复与迟滞增量

本轮是用户授权的“补齐 K1～K4 剩余核心”中的完整可验收增量，**不是整个 K1～K4 完成**。停机暂停的确定性时间协议、HoldFor/Debounce、冷却、静默/离线、告警生命周期、Resample 和生产环境门禁仍未完成；K5/前端未启动。

### 实现范围与兼容

- 新 profile9：线性 File＋静态 Lookup＋1～2 Count/IoT(TTL0)。原无状态引用仍为v8，不在旧profile中扩写状态。
- 新 profile10：线性 JetStream＋静态 Lookup＋0～2 Count/IoT(TTL0)，保存可靠输出cursor，epoch必须等于state generation。
- 新 profile11：required File→HTTP DAG＋静态 Lookup＋最多16个 Count/IoT(TTL0)，完整图语义、全部来源切点及精确表revision/SHA/runtimeCRC一起验证。
- 新 Hysteresis 使用 Bool latch/kind6。无引用 File使用v12、JetStream使用v13；有引用时使用9～11。参考表沿用CPL3，其他迟滞沿用CPL1，新外层version/profile保护旧reader；旧3～8的语义和已支持编码不改变，不自动迁移。
- 正TTL、ET/PT/temporal/Dedup、side/lossy图及JetStream DAG均不因本轮而开放。校验也覆盖手工构造/解码的时间参与者、新旧profile篡改、错误拓扑和外部owner依赖。

### 匹配功能与故障证据

服务器 `box@100.64.0.16`，证据根目录 `/workspace/bench-compare/k1-k4-core-artifacts-20260917/`；冻结 `package-v7-default` / `package-v7-jetstream`、`frozen-v7`，最终功能脚本 **`regression-v7d.exit=0`**。

| 验证 | 结果与边界 |
|---|---|
| Rust reliable/default-members | **726 passed、17 ignored**；ignored不当作通过 |
| 独立无demo | **44 passed**，不同feature组合，不与前者相加声称独立测试数 |
| 新专项 | 人工清单**26×20=520**，Plan4/Runtime15/Control7；9类fake inventory/summary反例通过 |
| 新真实进程 | Go v11 oracle，**9种**独立场景，另含旧B2对新9～13及新无引用profile的拒绝检查 |
| 旧矩阵回归 | B2 **25×20**、B1 **38×20**、A **8×20**、K4 **50×20**、K3 **24×20**及各自进程通过；K2 **32项**含broker与独立进程、default/K1 smoke及旧codec/catalog回退通过 |
| 静态与构建 | Clippy普通模式退出0，**100条warning**，不是`-D warnings`；Go vet/build、两套生产包及SHA校验通过 |

9种场景为 File Lookup→Count→IoT、File Count→Lookup、JetStream Lookup→Count、JetStream Lookup→IoT、Lookup→Branch→两个required HTTP、双来源Lookup→Union、Lookup→Count→Hysteresis→双required HTTP，以及无引用File/JetStream迟滞。

- table r1在r2/r3发布和GC后保持固定；进程读真实CPL3依赖、state count、Source cut，以及MAN2长度/分块CRC/PUBLISHED证明。
- HTTP body已接收但响应被hold时，CURRENT和broker ACK不得提前。先SIGKILL再释放夹具，从旧切点重放；Union验证各Source子序列，不假设全局排序。
- `CURRENT.tmp`目录注入真实I/O失败。File路径验证精确API错误；JetStream可能先向API waiter返回`cancelled: job stopping`，必须从最终actual状态验证`checkpoint io:`和`is a directory`，不能把任意取消算作预期故障。旧CURRENT及其发布payload保持，broker pending未被ACK，恢复后相同业务数据/输出ID重放。
- Store可在尝试提交前回收不受保护的旧generation以满足retention。本轮检查受保护CURRENT/payload，**不承诺失败提交保留全部可回收历史**；旧reader/profile拒绝路径则检查完整复制历史不变。
- 迟滞独立golden为57,60,55,60,54；保留Active后重放输出55,60,54。混合DAG在cut=3保存pending Count＋Active latch；输入6,7,1后输出13，后缀7,1,1,6,7输出2,13，强杀恢复两required sink仍精确输出2,13。

```text
base commit      1dd17c8186b5f52f2cea85a5fae1a94d79722b22
source manifest  0a53e656817e135de9f727e7b9e877dbaddbf7d926c5bf9f387a89689f9bdc8f
default server   cd9611457cd4e6fd49ce4f6bc06b0ac8991f58c71a24664b942df1603d4b7843
JetStream server b6e5712cc44f419a070c2d925f6be16af8f54ca615b6fb53d29e69b66d102750
Go v11 oracle   e0190687488ce2ef7d70cabae49359680a2f7e78d70c04648610cb8d60c6b914
```

Rust/生产二进制固定为v7；Go v11源码和binary另在`driver-v11/`、`k1-k4-reference-process-v11`归档。v7原始source manifest没有伪装包含后来的Go夹具修正。失败v1～v6构建、v7a～v7c进程记录保留：包括helper/import/有效ET图fixture、旧拓扑诊断优先级，以及停止后volatile状态已不可用、5秒采样指标不能当作实时ACK事实、可靠commit失败先取消API waiter等。只修正对应原因，未通过放宽运行时恢复规则制造PASS；未在本地运行Cargo。

复用入口（先在服务器独立构建Go oracle；验证脚本不编译）：

```sh
go build -trimpath -o DRIVER tests/k1-k4-reference-process/*.go
bash scripts/production-k1-k4-completion-validate.sh NEW_EVIDENCE PACKAGE_JS FROZEN DRIVER OLD_B2_SERVER NATS_SERVER 20
```

### 本轮性能与生产门禁

原K4 v5为fresh对照；周期仍为同候选on/off（100ms checkpoint、20ms应用响应等待）。预定完整三组ABBA，保持fresh≥0.97、periodic≥0.90、RSS增量≤2048KiB。三组单组及全样本合并均通过，`performance-v7.exit=0`，未删除任何试次：

| 负载 | 合并吞吐比 | RSS增量 KiB | 实测样本 |
|---|---:|---:|---:|
| fresh 零状态 | 1.014472 | 844 | 36 |
| fresh 双Count | 1.002904 | 568 | 36 |
| periodic 零状态 | 0.999027 | 180 | 24 |
| periodic 双Count | 1.000047 | 108 | 24 |

全部输出完整、无缺失/重复/非法行且对应hash一致；周期checkpoint每次测量成功数为正、失败0。这是本候选的File回归门槛，不是新增参考表路径的极限容量、WAN/p99或eKuiper新对照；也不改写历史候选的失败样本。

真实TLS/WAN、目标设备容量、24/72h与介质掉电模型仍为NOT RUN。未commit/push/tag或部署；既有Mosquitto PID2657777未被停止，没有修改host qdisc。
