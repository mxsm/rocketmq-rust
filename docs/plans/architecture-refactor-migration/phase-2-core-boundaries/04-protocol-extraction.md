# M04：`rocketmq-protocol` 边界提取

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 已批准，等待 M03 |
| 预计周期 | 2–3 周 |
| 工作包 | WP09 `protocol-boundary-spike`；承接 WP17 的 route/trace/wire 切片 |
| 前置条件 | model canonical 类型稳定；wire/public-path 基线可重复；M01 dependency guard 可登记新 crate |
| 可并行项 | schema 批次可在首个 request code 验证后串行推进；可与 M06 并行 |
| 完成后解锁 | M05、M07 |

## 目标

- 创建无 Tokio/TLS/common/legacy 的 `rocketmq-protocol`，拥有 wire schema、code、command、header/body 和纯 projection。
- 从一个真实 request code 开始，以 golden/differential test 证明新旧路径行为一致，再分批迁移。
- 让 `rocketmq-remoting` 在 R0/R1 只精确 re-export canonical protocol 类型。
- 把依赖 clock/env/file I/O 或 client state 的逻辑留在正确 owner，不做整目录盲搬。

## 非目标

- 不迁移 socket、TLS、connection、pending request、server runtime 或有状态 RPC client；它们属于 M05。
- 不改变 request/response code、header 语义、Serde 字段、Java hash、message body 二进制格式。
- 不把 `TraceDataEncoder`、TopicPublishInfo、route cache 或 static-topic 文件写入下沉到 protocol。

## 入口条件

- [ ] `[ARCH]` 选择一个覆盖 command/header/body 的真实 request code，冻结输入输出 golden。
- [ ] `[TEST]` 建立 canonical 与 legacy path 共用的 JSON/binary/error corpus，覆盖 `simd` 开关。
- [ ] `[DEV]` 确认 remoting/common/client 相关文件无用户修改重叠。
- [ ] `[HUMAN]` 批准任何无法保持 wire 等价的项目停止迁移，而不是修改协议适配设计。

## 交付物

| 类型 | 交付物 |
|---|---|
| Crate | `rocketmq-protocol`，`default = []`，仅依赖 model/error/macros 和必要 codec 值库 |
| Schema | code、RemotingCommand、header/body/admin/heartbeat/route/subscription/static-topic/RPC 值对象 |
| Pure logic | Java-compatible hash、wire key grammar、message codec、route/static-topic projection |
| Trace | 无 Client 状态的 TraceRecord/codec/constants；Client adapter 保持旧类型 |
| Compatibility | remoting/common/client 的精确 re-export/wrapper，一个 major 周期 |
| Tests | wire golden、differential、canonical/legacy compile fixture、`simd` on/off |

## PR 级开发步骤

### PR-M04-01：创建 crate 与单 request-code spike

- [ ] `[ARCH]` 固定 protocol 的模块入口、canonical root re-export 和禁止依赖。
- [ ] `[DEV]` 创建 crate 并加入 root workspace，继承统一 package/lint 元数据，`default = []`。
- [ ] `[DEV]` 迁移选定 request/response code、header/body 和最小 RemotingCommand 切片；旧定义改精确 re-export。
- [ ] `[TEST]` 同一 corpus 分别调用 canonical/legacy path，比较序列化 bytes、错误分类和 ext fields。
- [ ] `[REV]` 检查类型只定义一次，默认构造中的 clock/env 仍由旧 owner wrapper 注入。
- [ ] 回滚点：移除 crate/member/re-export，原实现恢复；调用方尚未批量迁移。

### PR-M04-02：迁移 model 前置的 wire primitives

- [ ] `[DEV]` 迁 Java-compatible hash、retry/POP key grammar、MessageSysFlag/TopicSysFlag、RocketMqVersion 和稳定协议常量。
- [ ] `[DEV]` common 原路径精确 re-export；动态 system-topic registry 和 owner-specific helper 留原 owner。
- [ ] `[TEST]` 固定 hash、bit flag、key、版本序号和字符串转换 golden，覆盖边界值。
- [ ] `[REV]` 检查 model 与 protocol 依赖单向，common 不被低层 crate 反向依赖。
- [ ] 回滚点：逐 primitive 迁移，任一差分失败只撤销对应切片。

### PR-M04-03：分批迁移声明式 schema

- [ ] `[DEV]` 按 code/command → header → body/admin → heartbeat/route → subscription/static-topic 的顺序迁移。
- [ ] `[DEV]` 每批先迁 canonical 定义和测试，再改 remoting 旧路径 re-export，最后迁 workspace 内部 import。
- [ ] `[DEV]` 清除 schema 中的 ArcMut、TimeUtils、EnvUtils 和 common helper；clock/env 默认值上移调用方。
- [ ] `[TEST]` 每批执行 JSON/binary golden、unknown/缺字段/边界长度负测和 canonical/legacy compile fixture。
- [ ] `[REV]` 检查 request code/Serde rename/default/skip 行为不变，不使用复制类型临时过渡。
- [ ] 回滚点：每批独立 PR；未完成批次继续由 remoting owner，不允许半迁移定义。

### PR-M04-04：拆分 static-topic、route 与 RPC 纯逻辑

- [ ] `[ARCH]` 将纯 mapping/check、需要 epoch 的 planner、临时文件 I/O 分为三个 owner。
- [ ] `[DEV]` 纯 static-topic mapping/route projection 进 protocol；epoch 由 admin-core planner 参数注入；文件 I/O 留 admin CLI。
- [ ] `[DEV]` codec-neutral RPC request/response/header 进 protocol；client metadata/hook/address resolution 留 M05 transport。
- [ ] `[TEST]` 对原工具函数与新投影做差分，确认 canonical projection 不原地排序或修改输入。
- [ ] `[REV]` 检查 protocol 无 clock/file/network/runtime 依赖。
- [ ] 回滚点：旧 `TopicQueueMappingUtils` 冻结为 deprecated wrapper；若 pure/impure 边界不清，停止该函数迁移。

### PR-M04-05：Trace record 与 message codec

- [ ] `[DEV]` 将纯消息二进制 decode 迁到 `body/message_codec`，message ID 语法继续由 model owner。
- [ ] `[DEV]` 新建不含 Client 状态的 TraceRecord/codec/constants；旧 TraceBean/Context/TransferBean/Type 和 encoder 留 Client adapter。
- [ ] `[TEST]` 固定分隔符、字段顺序、消息 decode 和错误输入 corpus；新旧 encoder 输出逐 byte 比较。
- [ ] `[REV]` 检查 AccessChannel、LocalTransactionState、callback/runtime 未进入 protocol。
- [ ] 回滚点：保留旧 encoder，撤销 canonical codec 调用；不得保留两套可独立演进的 wire schema。

### PR-M04-06：feature/facade 与全量收口

- [ ] `[DEV]` protocol 拥有 `simd`；remoting 仅做 `simd → protocol/simd` 弱转发，保持当前 feature 名和默认语义。
- [ ] `[DEV]` remoting 根/prelude/深路径精确 re-export；已迁实现不得留在 facade。
- [ ] `[DEV]` 更新 dependency guard；workspace 实际 package 数应在 M03 基础上增加 1。
- [ ] `[TEST]` 执行 `simd` off/on、all-targets、canonical/legacy path 和受影响 consumer。
- [ ] `[REV]` 用 `cargo tree` 证明 protocol 无 common/legacy/Tokio/TLS/transport/client/server/auth/store。
- [ ] `[HUMAN]` 批准 protocol contract 冻结后解锁 transport 提取。

## 公共兼容面

- request/response code、header、Serde field/default、JSON/binary bytes、RocketMqVersion 和 public enum 值不变。
- `rocketmq-remoting` 的根级与已公开深路径保留一个 major；canonical 定义只存在于 protocol。
- `simd` feature 名称和 remoting 转发保持；protocol 自身空默认 feature。
- static-topic 中依赖时间和文件系统的旧 API 通过 deprecated wrapper 保持，内部调用改用显式参数/CLI owner。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-remoting
cargo test -p rocketmq-client-rust --lib
cargo test -p rocketmq-admin-core
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
.\scripts\check-error-hygiene.ps1
git diff --check
```

### 本里程碑新增后执行

```powershell
cargo check -p rocketmq-protocol --no-default-features
cargo test -p rocketmq-protocol --no-default-features
cargo test -p rocketmq-protocol --features simd
cargo tree -p rocketmq-protocol -e normal
python scripts/architecture_dependency_guard.py --mode baseline
```

wire golden/differential 的命名测试在落地后加入证据索引；未创建测试前不把示例名当成可运行命令。

## 回滚触发器

- 任一 golden byte、request code、Serde 语义或错误分类发生未批准差异。
- protocol dependency tree 出现 Tokio/TLS/common/legacy/transport/service。
- canonical 与 legacy path 形成两个定义，或 feature 转发改变默认构建。
- static-topic/trace 迁移把 clock/file I/O/client state 带入 protocol。

按 schema 批次回滚：先恢复 legacy re-export 到原定义，再回滚 canonical 切片和 consumer import。已发布 canonical crate 不做强制历史重写；通过 patch release 恢复兼容。

## Exit Checklist

- [ ] `[TEST]` 所有 wire golden/differential 在 `simd` off/on 一致。
- [ ] `[REV]` protocol normal closure满足所有禁边且无状态 owner 泄漏。
- [ ] `[DEV]` remoting 旧路径精确 re-export，类型只定义一次。
- [ ] `[TEST]` canonical/legacy compile fixture 和受影响 consumer 全绿。
- [ ] `[ARCH]` pure projection 与 owner wrapper 边界有记录。
- [ ] `[DEV]` dependency policy 与实际 workspace package 数一致。
- [ ] `[HUMAN]` wire/API 兼容证据已签署。

## 交接物

- 向 M05 交付稳定 protocol API、feature 转发、RPC 值对象和 wire corpus。
- 向 M07 交付 route/trace/static-topic canonical 路径和兼容 adapter。
- 向 M09 交付 remoting facade ledger 和下一 major 删除清单。
