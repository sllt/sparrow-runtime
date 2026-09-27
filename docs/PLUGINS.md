# 插件：共同管理与可信原生函数 Preview

第8批首个完整子批：包安装/校验、版本固定、启停/查询、显式升级回退，以及SQL/Graph调用原生标量函数。**2026-09-27 实现、自查和限定服务器验证完成，尚未发行。** 脚本、WASM、Transform和Source/Sink外部插件尚未实现，不能把本子批算作整个插件体系完成。

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

下一子批为脚本函数，其后WASM，最终完成Transform/Source/Sink SDK及必要隔离。签名信任链、任意native隔离/强制终止、外部Connector恢复、自定义UDAF不因这次scalar通过而自动获得支持。
