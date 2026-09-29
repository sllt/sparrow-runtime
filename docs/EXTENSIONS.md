# 外部 Source / Sink / Transform SDK（v1 Preview）

这是 Sparrow 自有的、独立进程的协议 SDK，不是 eKuiper Go 插件二进制兼容层，也不是 Node-RED/Web 平台。SDK `crates/sparrow-extension-sdk` 只依赖 serde/serde_json/libc，独立示例项目 `examples/extensions` **不链接 Server、runtime、model**。第三方也可按协议自行实现，无需使用 Rust ABI。

## 启用及交付

Linux GNU ELF64，manifest 精确指定 x86_64/aarch64；发行验收以实际测试架构为准。默认关闭：

```sh
export SPARROW_PLUGIN_DIR=/var/lib/sparrow/plugins
export SPARROW_ENABLE_EXTERNAL_PLUGINS=1
bash scripts/build-extension-example.sh /tmp/new-extension-package
sparrowctl plugin-install /tmp/new-extension-package/source.json /tmp/new-extension-package/artifact.elf
sparrowctl plugin-enable APPROVED_MANIFEST_SHA256
```

分别安装、批准 `source.json`、`sink.json`、`transform.json`。示例同一 executable 提供三种角色，**每角色独立 manifest/hash**。安装支持第三个 detached signature 参数；格式2包的依赖、Ed25519信任策略、历史revision引用保护与标量插件完全共用，见 [PLUGINS](PLUGINS.md)。在 `enable` 时启动短命探测进程，Describe 必须逐项等于批准的声明，随后 Close；不传业务 config、不调用 Open。批准原生代码前仍须审查其启动代码。

生产包附带 SDK/示例源码与锁文件、构建脚本及 SHA256SUMS。故障示例仅通过独立项目 `--features conformance --bin sparrow-extension-fault` 构建；正常构建脚本不会生成它。不在线下载或执行包依赖；依赖是部署及生命周期约束，不是插件间 RPC 或动态链接导入。

升级前同时备份catalog、插件目录和信任策略。catalog仍是v4，但旧版本不认识新的extension声明及管道绑定；回退必须恢复配套备份，或先停止并retire所有相关历史、停用并卸载外部包，不能仅替换旧binary后声称兼容。

## 配置及 schema

Source/Sink 使用 `kind:"plugin"`，`plugin` 是 `{name,version,manifest_sha256,config}`。config 必须 object/null，编码最多4KiB；不自动展开环境变量、Secret 引用或 action 模板。常规 connector 的 host/path/tls/action 等混用参数拒绝，显式业务选项只放入 config。此配置持久化在管道revision中，**不要把原始密码放入config**。

```json
{
  "kind": "plugin",
  "inbox_capacity": 1,
  "plugin": {
    "name": "example_source", "version": "v1",
    "manifest_sha256": "REPLACE_WITH_APPROVED_HASH",
    "config": {"start": 1, "count": 35}
  }
}
```

示例 Source 输出一个非NULL int64 `value`；Sink 接收同名可NULL int64，config `{ "path":"/absolute/new-file.ndjson" }`，只创建新文件、不覆盖，Flush 做 `sync_data`。Transform config `{ "factor":2,"copies":2 }`，checked multiply，NULL保留，最多16行展开。

Graph 节点写成 `{ "id":2,"kind":"plugin_transform","plugin":BINDING,"out":[3] }`，需单输入；普通 Source/Capture节点配合 `graph_io` 可混合外部和内置 connectors。SQL仍使用原有内置算子/标量函数；多行 Transform 是 Graph 算子，不伪装成纯标量SQL函数。

每端口最多16列，类型 Bool/Int64/UInt64/Float64/Utf8/Bytes/TimestampMicrosUtc，无 Dynamic/Array。整数/时间在 wire 上用十进制字符串，浮点必须有限，Bytes 为字节数组。列名、顺序、类型逐项匹配；输入声明可接收更窄的NULL范围，输出不能比接收schema更宽。声明不是隐式类型转换。bind/启动以及每次返回均复核。

## 协议与生命周期

完整 wire enum 在 `crates/sparrow-extension-sdk/src/protocol.rs`：私有 stdin/stdout，4字节 big-endian frame length＋严格JSON；协议 `sparrow-extension-ipc-v1`，单session递增sequence、单在途请求。stdout禁止日志；stderr不转发业务错误，错误只返回固定 code。

- `Describe → Open → Poll/Data/Accepted/Idle/End`：仅Source。一次Data未被Accepted前不得再次Poll；宿主把完整校验后的rows及其watermark按顺序交给有界内存队列后才Accepted。**这只是易失队列准入，不是broker ACK或持久checkpoint提交**。
- `Push → Accepted`：仅Sink。当前宿主每次一行，单在途、无batch/linger或重试。断链/超时有不确定的外部结果，不假定业务写入回滚。
- `Transform → Rows`：每个输入返回0..声明上限行；整份结果校验并取得输出credit后才发布。必须无业务I/O、无跨调用业务状态；纯度是可信原生合约，不是OS强制证明。
- `Flush → Close`：正常Sink EOF及可用session的停止路径有界flush/close；损坏/超时session直接kill并wait，不在歧义失败后重新发送业务操作。Close后不再调用。取消不承诺排空尚未消费的输入。
- Source的watermark需显式声明能力、单调递增；Data中row在watermark前入队。End明确声明有限输入完成，宿主终结ET窗口；Graph另发EOF，线性流用channel关闭。普通Idle是轮询退避，不是event-time分区idle声明。

Transform保持origin/source_operator，按输入顺序转发正常watermark/idle/active/EOF；不接受checkpoint或processing-time/feed controls。当前仅 `live_best_effort + restart_fresh`；拒绝aligned、restore、checkpoint设置、依赖观测/processing-time的专用控制profile。有限查询API不执行这些可信原生Transform。失败或进程重启后**必须显式start**，不自动重新从Source开头回放；显式start仍可能重复输出，业务需自行去重。

## 资源、隔离与信任边界

每服务进程最多8个external session（独立于JS/WASM共用的4个worker名额）；每session独立子进程，host每次IPC固定1s期限，取消轮询、kill进程组并wait后释放名额；SDK回调另有2s alarm兜底。manifest最多32行、每frame1..64KiB，host在分配/发送前和解析时限流；Source的Poll进一步收紧到Kernel ingress批次上限。IPC、解码和转换scratch先从Job reservation取credit；额度不足先拒绝，不当作无限内存。

子进程使用校验后的sealed memfd，清空环境、cwd=/、不继承非标准FD；设置 no_new_privs、独立进程组、RLIMIT_AS=128MiB、NOFILE=16、CORE=0、NPROC=0。GNU平台依赖按已有ELF检查规则白名单校验；不递归认证管理员提供的libc。

**不是完整OS/多租户沙箱**：原生程序仍具有服务UID的文件/网络权限；声明 `filesystem/network` 用于批准与可见性，不是系统调用过滤器，也不受内置 connector TargetPolicy 自动约束。不要加载不可信原生程序，不以root服务身份承诺NPROC约束，恶意原生程序逃离进程组不在此安全保证内。128MiB是每个子进程地址空间上限，不是整个服务RSS预算。恶意代码/强隔离部署仍需额外容器、UID、seccomp/cgroup/网络策略。

管理列表展示声明、hash、签名、desired/enabled/pin与 `external_sessions`；`resident`是已装载sealed executable，不等同于正在运行子进程。Connector状态提供 `plugin_source_rows`、`plugin_sink_rows`、`plugin_polls`、`plugin_failed` 与health。Source行数是已接受的易失准入，Sink行数是收到Push响应，不是端到端持久回执。plan/session pin未释放时禁止停用/卸载；所有历史revision还保护卸载，停止并显式retire后才能释放目录引用。

## 验证入口

- SDK常规协议测试随workspace测试运行。
- `sparrow-plugin --test extensions` 与Control `extensions_control_*` 为显式fixture测试：先单独构建示例及conformance binary，设置 `SPARROW_EXTENSION_EXAMPLES` / `SPARROW_EXTENSION_FAULT`，再带 `--ignored --test-threads=1` 执行，不静默跳过缺失fixture。
- `scripts/plugins-extensions-smoke.sh` 使用隔离catalog、测试密钥、新输出目录执行真实Server/CLI升级回退、失败与safe-mode流程。测试日志/私钥/二进制不提交。
- 本节是支持边界和复现入口，不代表24小时soak、任意第三方插件、aarch64或整体生产认证已经通过；匹配测试证据单列在 [PRODUCTION](PRODUCTION.md)。
