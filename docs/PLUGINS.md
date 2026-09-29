# 插件：共同管理、可信原生与 JavaScript 函数 Preview

第8批按子批交付：共同管理＋可信原生标量（2026-09-27），随后 JavaScript 标量（2026-09-29）。支持包安装/校验、版本固定、启停/查询、显式升级回退与 SQL/Graph 调用。**仍为未发行的 Development Preview**，验证范围见 [PRODUCTION](PRODUCTION.md#script-plugins-validation)。WASM、Transform 和 Source/Sink 外部插件仍待实现，不能把标量子批算作整个插件体系完成。

## 信任与部署

默认不配置插件目录、不加载任何原生代码。显式设置：

```sh
mkdir -m 700 /var/lib/sparrow/plugins
export SPARROW_PLUGIN_DIR=/var/lib/sparrow/plugins
export SPARROW_ENABLE_NATIVE_PLUGINS=1
```

目录属于服务用户，不接受目录symlink、组/其他用户可写目录，单进程持锁。开启目录但不开native开关时可安装/查看，不可激活；safe-mode不自动恢复激活，也拒绝enable。已批准enabled版本会在后续正常启动、Supervisor启动之前校验并加载。缺包、损坏、ABI/平台不符明确失败，不自动切换版本。

**只运行已审查的可信本地代码**。认证管理员必须再次批准完整manifest SHA-256；hash证明所选字节/声明身份，不证明作者或安全性。本版不实现数字签名验签、插件商店、依赖下载。原生代码与进程同权限，可以破坏进程，不能强制中断、不能靠catch_unwind提供沙箱；原生内部堆分配、线程、I/O也不受宿主行预算强制管控。ABI要求纯函数、线程安全、不保留宿主指针、不跨边界抛异常、及时返回，只是可信代码合同。因此 `/v1/query` 明确拒绝native调用，不冒充可抢占的有限查询。

首版ABI为Linux ELF64 little-endian GNU、x86_64/aarch64匹配目标；平台声明不是两种架构均已实测。manifest最大16KiB，artifact最大4MiB，安装最多16个包版本；HTTP上传最多6MiB，单个插件管理请求准入，普通管理接口仍为64KiB。每进程最多16次原生驻留加载尝试，失败也可能执行过构造函数，所以同样占名额。

name/version/function标识均为1～32个小写字母、数字或下划线（例如`v1`），不是自动解析semver。第三方动态依赖不被打包、下载或递归hash固定，平台动态库属于管理员维护的部署基线；artifact hash不等于整个进程依赖树的证明。备份/回退必须同时保留catalog和插件目录，在停止管理变更后操作，不能只备份SQLite而遗漏规则固定的包。

库从**验证hash后的sealed memfd**加载，不把可被替换的磁盘路径交给dlopen。库和sealed fd驻留至进程退出，不调用dlclose；宿主不承诺安全热卸载。disable只阻止新绑定，存在plan/job引用则拒绝；disable后需重启才能通过管理API卸载该驻留版本。失败激活不得反复重试耗尽句柄，须重启检查。API会在跨入原生构造函数之前记录批准尝试，失败时不能只依靠成功后的审计。`resident`对已尝试激活的版本采取保守标记，不是实时调用或精确RSS指标。

## 构建、安装和调用

可运行C样例和ABI头分别位于 `examples/plugins/native_math.c`、`sdk/native/sparrow_plugin_v1.h`：

```sh
bash scripts/build-native-plugin-example.sh /tmp/native-math-v1
sparrowctl plugin-install /tmp/native-math-v1/manifest.json /tmp/native-math-v1/native_math.so
sparrowctl plugins
sparrowctl plugin-enable MANIFEST_SHA256
```

install不执行代码，成功返回manifest、`manifest_sha256`、enabled/resident/pins。enable参数是install返回的**manifest hash**，不是artifact hash；CLI会把该值作为明确批准提交。HTTP使用现有Bearer认证：

- `GET /v1/plugins`
- `POST /v1/plugins/install`：`{"manifest":{...},"artifact_base64":"..."}`，严格JSON/base64；无URL下载或任意服务端文件路径。
- `POST /v1/plugins/{manifest_sha256}/enable`：`{"approve_manifest_sha256":"同一完整hash"}`。
- `POST /v1/plugins/{manifest_sha256}/disable`、`.../uninstall`。

SQL：

```sql
SELECT plugin_call('native_math','v1','完整manifest_sha256','double',value) AS doubled FROM s
```

前四个参数必须是字符串字面量，后面0～8个声明类型的参数。Graph使用同名 `{"k":"call","name":"plugin_call","args":[...]}`，四个固定参数是普通UTF8 literal ExprSpec。SQL/Graph绑定到该Catalog的registry，不使用进程全局的可变函数名表；绑定表达式持有不可变函数句柄，行执行不查可变registry。

每个Job最多64个调用点，固定依赖持有至整个Job结束；编辑/验证中的plan也会短暂pin。升级安装新version，更新规则的version与完整hash再启用；回退选回原version/hash。没有latest别名，不修改正在运行的规则。允许停用未使用版本，使用中的版本不得替换、停用或卸载。同name/version内容不可覆盖；即使卸载后重装同版本，旧规则仍因hash不同而拒绝漂移。

## ABI与资源

ABI v1使用固定C-layout tagged scalar structs和host提供的输出buffer；不传Rust trait object/Vec/String，不跨边界free。NULL/Bool/Int64/UInt64/finite Float64/UTF8/Bytes/Timestamp微秒受支持，nested/Dynamic暂不开放。输出检查tag、reserved、长度、UTF8、bool和非有限浮点；每值文本/bytes最多64KiB，函数声明输出上限。参数先按表达式顺序求值并验证；NULL传播且不调用native。错误返回非零状态并结束对应规则，不静默吞错。

输入字节只借用至本次调用结束，输出detach到宿主Scalar；host buffer及结果通过既有AllocationBound预留，plan/compiled表达式及Job pins有额度。恶意native可以无视buffer容量、留存指针或自行分配，这些不由C ABI校验变成可防御行为。只有受测可信代码适用本Preview。

artifact大小/加载次数上限不是进程RSS上限；ELF段、系统依赖和原生内部资源不计入Job credits，不能把同进程原生插件当成受宿主硬配额约束的执行器。

插件表达式**restart_fresh only**，Control、CheckpointPlan、Kernel和旧状态语义构建均拒绝aligned/restore；不为旧快照添加codec。管理操作使用原有认证和有界blocking worker；请求超时或磁盘错误不代表操作已回滚，尤其enable可能已运行构造函数。存储故障尚未做完整验收，出现写入/fsync错误应停止管理变更、检查存储并在重启后核对包状态，不能假定内存列表已证明磁盘结果。

## 测试与后续

服务器Linux x86_64/Rust1.98.0已通过：默认13成员/JetStream Release **970 passed / 21 ignored**，无demo Server/CLI **47 passed**，新13项重复5轮 **65 passed**。覆盖真实ELF加载、7种scalar tag/NULL/溢出/错误输出、hash/版本/ABI拒绝、目录锁/symlink/孤儿安装、升级/重开/回退/卸载、SQL/Graph隔离、运行Job pin/取消/退款、真实File→native→HTTP、API认证/上传限额/safe-mode/有限查询拒绝和旧回归。

真实no-demo Server/CLI进程也通过：File→native→File，同一输入21在v1/v2/v1输出 **42→63→42**；运行中停用拒绝、safe-mode不自动加载、正常重启恢复启用、驻留卸载拒绝、停用重启后卸载、缺包拒绝。可复现脚本为 `scripts/plugins-native-smoke.sh SERVER CLI NEW_EVIDENCE_DIR`。初轮脚本权限/CAS失败保留，不放宽生产校验；最终源码、二进制和日志指纹见[匹配证据](PRODUCTION.md#native-plugins-validation)。未测aarch64、长稳、ABI恶意机器码隔离、掉电存储故障或性能/整体发行认证。

后续为 WASM、Transform/Source/Sink SDK 及必要隔离。签名信任链、任意 native 隔离/强制终止、外部 Connector 恢复、自定义 UDAF 不因 scalar 通过而自动获得支持。

## JavaScript 标量函数

引擎固定为 **QuickJS-ng 0.16.2（rquickjs 0.14.0）**，不是 Node.js、浏览器或 Boa。引擎只链接进独立的 `sparrow-js-worker`，不进入 Server/CLI 的进程地址空间。现阶段支持 Linux GNU x86_64/aarch64，实机验证仅 x86_64；要求 Linux `close_range` 支持（5.9+）。

```sh
export SPARROW_PLUGIN_DIR=/var/lib/sparrow/plugins
export SPARROW_ENABLE_SCRIPT_PLUGINS=1
# 默认寻找 sparrow-server 同目录的此文件，也可显式指定绝对路径。
export SPARROW_JS_WORKER=/opt/sparrow/current/bin/sparrow-js-worker
bash scripts/build-script-plugin-example.sh /tmp/script-math-v1
sparrowctl plugin-install /tmp/script-math-v1/manifest.json /tmp/script-math-v1/script_math.js
sparrowctl plugin-enable MANIFEST_SHA256
```

管理员负责安全安装 worker 及其父目录；worker 必须为 root/服务用户所有、非 symlink、非 setuid/setgid、不可被组/其他用户写入的可执行普通文件。生产构建脚本一并打包 worker、JS 示例和生成 manifest 的脚本。**不需要启用原生插件开关**；safe-mode 同样阻止脚本自动激活和手工 enable。默认 systemd 示例使用 safe-mode，启用插件须显式选择非 safe-mode 的部署配置。

manifest 使用 `kind:"javascript_scalar"`、`target:"javascript-quickjs-ng-0.16.2-v1"`，其余共用 ABI/semantics v1、函数类型声明及 NULL 传播合同。源文件最多 32KiB UTF-8；最后一个表达式返回按声明名称导出函数的对象，不是 ES module：

```javascript
({
    double(value) { return value * 2n; },
    upper(value) { return value.toUpperCase(); }
})
```

SQL/Graph 调用方式不变，例如 `plugin_call('script_math','v1','完整manifest_sha256','double',value)`。Int64、UInt64、微秒 Timestamp **精确映射为 BigInt**，须用 `2n` 而不是 `2`；返回 Number 不会隐式截断为 64 位整数。Float64 对应有限 Number，Bool 对应 boolean，UTF8 对应 string，Bytes 对应 Uint8Array。严格拒绝整数溢出、类型错误、非有限浮点和非法 UTF-16 字符串；返回 null 允许传播，undefined、Promise 和其他对象不是标量输出。

### 隔离、配额和故障

- 每个已激活版本独占一个复用的执行进程，同版本调用串行；全服务进程最多 **4 个脚本 worker 名额**。名额与原生 16 次驻留加载尝试独立。仍可安装最多 16 个包版本，但未使用的脚本版本应停用以释放名额。
- **每次调用新建 Runtime＋Context**，不共享全局变量、原型修改、闭包状态或 Promise 队列；不执行异步 jobs。启用时由同一 worker 编译 bootstrap 和已校验源文件，仅缓存生成的不可变 bytecode，合计最多 **256KiB/worker**；调用时重载，仍重新执行顶层初始化。没有跨调用实例池、磁盘缓存或外部 bytecode 上传接口。保留严格模式、Function.toString 的源文本与调试位置。
- `script_cache` 返回 artifact SHA-256、缓存字节数和编译脚本数（2），停用后为 null；是加载元数据，不是 worker 存活证明。缓存包含在 128MiB 进程上限内，不计作宿主 Job owner 内存。重启/重新启用会从源码重新编译。内部 IPC 升级为 `sparrow-js-quickjs-ng-0.16.2-ipc2`，须配套部署 Server 与 worker；源文件 manifest target/hash 合同不变，不接纳旧协议 worker。
- worker 地址空间 `RLIMIT_AS=128MiB`（不是 RSS 预分配），每个 QuickJS Runtime 堆上限 64MiB、JS 栈上限 1MiB。主机全 Job 的 IPC 编码/解码/返回值另按函数类型计入既有 reservation；worker 自身的地址空间**不冒充**已由 Job `MemoryOwner` 记账。部署总内存预算须包含子进程。
- 每次调用最多 8 个参数、总标量 payload 最多 64KiB；文本/Bytes 输出受函数声明上限约束，最高 64KiB。IPC frame 最高 512KiB，实际响应还按声明收紧；长度检查先于主进程分配。
- 调用期限 **100ms**，包含等待同版本的其他调用和 IPC；启用验证期限 2s。内层 QuickJS 中断只是辅助，主进程轮询期限/Job cancellation 后 kill＋wait 回收进程，防止正则等内建长操作绕过中断。不是实时调度 SLA，也不能消除阻塞内核 I/O 的影响。
- worker 自带调用 1s/初始化 3s 的 SIGALRM 兜底；父进程消失时，空闲 worker 由 stdin EOF 退出，忙碌 worker 由自身 watchdog 终止。没有使用 Linux 绑定到创建线程的 PDEATHSIG，避免 Tokio 临时 blocking 线程退出误杀 worker。
- 清空 worker 环境并关闭继承的非标准 fd，不注册 FS、网络、模块 loader 或宿主回调；关闭 eval/Function 及 async/generator constructor 动态编译路径、Date、Math.random、SharedArrayBuffer/Atomics 等入口。**资源/进程隔离不是 seccomp、namespace 或不可信多租户的完整 OS 沙箱**；worker 仍以服务用户运行，不承诺防御引擎漏洞。代码仍需管理员审查及精确 hash 批准。
- 脚本异常让对应 Job 失败，不静默丢弃/吞错。持有 worker 的调用超时、取消、worker 退出或协议故障会把该版本的 worker 标记为 failed，不自动无限重启；排队前/期间被拒绝不会杀死另一调用的 worker。异常被正常返回时 worker 可继续使用；已失败版本先停止使用者，再 disable→enable 恢复。其他版本的 worker 独立。`script_worker_state` 为最近观测到的 ready/busy/failed/unloaded 状态，不是周期 liveness probe。
- disable 仍拒绝存在 plan/job pin 的包；脚本无 pin 时会回收进程，随后可以**不重启 Server 就卸载**。原生仍必须停用后重启，不能混同。顶层 `hot_unload:false` 表示并非所有后端支持；以包的 `hot_unload` 或 `script_hot_unload` 为准。

### 有界诊断与有限查询

错误上下文包含 `script_phase`（compile/initialize/export/arguments/call/result/cache/runtime），可取得位置时再包含 `script_frames`，最多 8 个 `行:列`，均从 1 开始，列按源码 UTF-16 计。不返回异常 message、文件路径、函数名或业务参数。worker 在执行用户代码之前保存原生 Error stack getter，只对引擎直接确认的 Error 对象调用它，不访问自定义 stack/message getter、Proxy 或任意对象的 toString。非 Error、超大栈或没有有效源码坐标时省略 frames；自定义 Error.prepareStackTrace 可以影响坐标，因此位置仅作诊断提示，**不是可信来源证明**。

`/v1/query` 与 `sparrowctl query` 允许已启用、精确版本/hash 固定的 JS 标量。每个非 NULL 调用预扣 **10,000 + 参数 payload 字节数**，与所有 stage 共享整次查询的 `limits.work_units`，失败/排队尝试不退款；NULL 传播不进入 VM。它是保守的调用准入成本，**不是 VM 指令计量器**。默认 1,000,000 work units 还需支付普通算子成本，因此不足 100 次整型调用；上限 10,000,000。嵌套调用、跨行/跨 stage 不重置预算。

查询还共享一个绝对执行截止时间，每次调用实际期限取查询剩余时间与 100ms 的较小值；取消/失败后 join 全部任务才释放查询准入，不返回部分成功结果。原生或其他不可抢占插件仍拒绝进入有限查询。脚本仍 **restart_fresh-only**，不能用于 aligned/checkpoint 恢复。WASM、外部 Source/Sink、批量 IPC、实例池、长期 soak、aarch64 验证另行交付。

### 为什么最终选择 QuickJS

同服务器、128MiB 进程地址空间、100ms 父进程期限、复用进程但每请求新 Context；ABBA 顺序、6 种正常 workload，每轮每项 1,000 次（另预热 20 次），共 24,000 次。QuickJS 同时开启 64MiB Runtime 上限和中断回调。各轮 p50 的均值：

| 场景 | Boa 0.22.0 | QuickJS-ng 0.16.2 |
|---|---:|---:|
| BigInt 计算 | 381μs | 143μs |
| JSON 业务规则 | 452μs | 153μs |
| 64KiB JSON 字符串 | 1,917μs | 627μs |

这是包含测试 IPC、新 Context 和解析/执行的微测试，**不是完整 Sparrow 流水线吞吐，也不是复用上下文/字节码缓存的排名**。同口径 QuickJS 约快 2.5～3 倍；两者的复杂正则都需要父进程兜底。Boa 初轮 10k 循环预算误拒绝 64KiB JSON（内置函数也消耗预算），调整为 1M 后重跑全部普通用例通过，并非把初轮配置失败算作引擎缺陷。完整版本、配置、源码/二进制指纹及原始失败日志见 [匹配证据](PRODUCTION.md#script-plugins-validation)。
