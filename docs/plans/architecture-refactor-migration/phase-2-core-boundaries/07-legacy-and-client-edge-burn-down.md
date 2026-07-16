# M07：Legacy Runtime 排空与 Client 依赖边收敛

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 进行中；PR-M07-01、PR-M07-02 已完成 |
| 预计周期 | 3–4 周 |
| 工作包 | WP15 `rocketmq-rust-drain`、WP18 `client-edge-burn-down`；完成 WP17 的 consumer 迁移 |
| 前置条件 | model/protocol/transport/store-api canonical 边界稳定；Client allowlist/source guard 可用 |
| 可并行项 | Legacy、Broker、NameServer、Admin、Dashboard 按项目可并行，但共享 admin-core/model/protocol 变更须串行 |
| 完成后解锁 | M08、M09 |

## 目标

- 将 `rocketmq-rust` 的 schedule/task/shutdown/signal 能力迁入 runtime，新 crate 禁止依赖 legacy 层。
- 把完整 Client 的直接依赖和 import 收敛到 workspace 的 proxy-cluster、admin-core/client_adapter，以及 standalone example。
- Broker/NameServer 改依赖真实 owner capability；MCP/Dashboard 经 admin-core 间接访问。
- 在 manifest、源码 import 和 normal transitive closure 三个层面证明边界，而非只删一个 Cargo 行。

## 非目标

- 不删除 `rocketmq-rust` crate 或外部公开兼容路径。
- 不创建 `client-api`；不让 Broker/NameServer 通过 admin-core 绕行。
- 不在本里程碑完成 Proxy 三向物理拆分；为 M08 清理 DTO/port 前置条件。
- 不把所有 Dashboard 业务重写，只迁移 Client lifecycle 与管理调用 owner。

## 入口条件

- [ ] `[ARCH]` 对每条当前 Client 直接边指定目标 owner 和 adapter，冻结禁止绕行规则。
- [ ] `[TEST]` 从各项目最近的 `AGENTS.md` 生成根、Example、Tauri、Web 的累计验证路线；仅当本里程碑实际修改 `rocketmq-dashboard-common` 时，额外加入 GPUI 条件验证。
- [ ] `[DEV]` 分别检查根和 standalone 目标文件的用户修改，不跨 writer lease 修改。
- [ ] `[HUMAN]` 批准 admin-core R0 feature：legacy compatibility 显式蕴含 client-adapter，下一 major 删除。

## 交付物

| 类型 | 交付物 |
|---|---|
| Legacy | runtime-owned schedule/task/shutdown/signal；legacy 旧路径 re-export；新 crate 禁边 |
| MCP | 删除未使用的 Client manifest/source 直接边，QueryFacade/AdminSession 行为不变 |
| NameServer | 注入 `ClusterTestRouteLookup`，protocol+transport adapter，3 秒 deadline |
| Broker | model/protocol/store-api owner-local result/route/read projection，完整 Client 清零 |
| Admin | admin-owned capability/DTO/Clock；Client import 只在 `src/client_adapter/` |
| Dashboard | Web/Tauri 经 admin-core/dashboard-common，无自建 producer/client lifecycle |
| Guard | Client manifest/source allowlist和关键 normal closure assertion |

## PR 级开发步骤

### PR-M07-01：`rocketmq-rust` 生命周期能力迁入 runtime

- [x] `[ARCH]` 固定 schedule/task/shutdown/wait-for-signal 的 owner、public compatibility 和退出顺序。
- [x] `[DEV]` 等价迁移到 `rocketmq-runtime`；legacy 路径精确 re-export，不迁 ArcMut/WeakArcMut/SyncUnsafeCellWrapper。
- [x] `[DEV]` workspace 内部 consumer 改用 runtime canonical path，新 crate policy 禁止依赖 `rocketmq-rust`。
- [x] `[TEST]` 对 schedule/cancel/signal/shutdown 做新旧路径差分和 runtime audit。
- [x] `[REV]` 检查无第二套 RuntimeOwner、无 detached task/thread、legacy 无新 owner 代码。
- [x] 回滚点：consumer 恢复 legacy import，canonical runtime API 保留；不恢复已删除的危险 ArcMut escape。

### PR-M07-02：删除 MCP 冗余 Client 边

- [x] `[DEV]` 证明 MCP 源码未直接使用完整 Client 后删除 manifest 边；QueryFacade/AdminSession 继续经 admin-core。
- [x] `[TEST]` 运行 MCP check/test/streamable-http Clippy/doc 和默认 tool/resource contract。
- [x] `[TEST]` 确认 8 个默认只读/诊断 Tool 与 5 个 change-planning Tool 仍 `mutates_cluster: false`。
- [x] `[REV]` 检查没有通过 common/remoting re-export 绕过 source guard，stdio stdout 仍只含协议帧。
- [x] 回滚点：若 admin-core 缺少合法 capability，先扩展 admin-owned port；不恢复 MCP 直接 Client 边作为快捷方案。

### PR-M07-03：NameServer RouteLookup 反转

- [ ] `[ARCH]` 固定 `ClusterTestRouteLookup` 输入/输出、3 秒 deadline、缓存/回退和生命周期 owner。
- [ ] `[DEV]` 默认 adapter 只依赖 protocol+transport，由 ServiceContext 拥有；移除 MQClientManager/admin registry。
- [ ] `[TEST]` 对成功、timeout、NameServer 不可达、route 回退、shutdown 做差分。
- [ ] `[REV]` 检查 NameServer manifest/source/normal closure 无 Client，deadline 不在每层重置。
- [ ] 回滚点：旧 processor facade 保留；adapter 可切回旧实现，但 Client 边重新出现会阻塞 M07 Gate。

### PR-M07-04：Broker Client 边清零

- [ ] `[DEV]` 远程结果改用 model；本地 transaction/POP 改用 store-api read outcome。
- [ ] `[DEV]` 建 Broker-owned `BrokerPublishRoute` 和 out-api send/pull/result adapter；TopicPublishInfo 留 Client。
- [ ] `[DEV]` 迁 query assignment 分配算法、route projection、escape bridge 和 test-only fixture。
- [ ] `[TEST]` 覆盖 send/pull/pop/transaction/route/query-assignment 差分和 all-targets 编译。
- [ ] `[REV]` 检查 Broker manifest/source/normal closure无 Client/client-api，processor 不依赖 store facade。
- [ ] 回滚点：按 result/route/assignment/read 投影切片回滚；不得新增 client-api 或 admin-core 绕行。

### PR-M07-05：Admin contract 与 Client adapter 收口

- [ ] `[ARCH]` 按 Topic/Broker/Consumer/Security/Lite 拆 admin-owned capability/request/result，冻结 R0 legacy surface。
- [ ] `[DEV]` 建 `core/` 与 `client_adapter/` 同领域模块；所有 MQAdminExt/producer 调用只存在于 adapter。
- [ ] `[DEV]` 注入 admin-owned Clock 替代 TimeUtils；static-topic 纯 planner 留 core，SDK 调用进 adapter，文件 I/O 留 CLI。
- [ ] `[DEV]` 实现 `client-adapter` optional feature；`legacy-common-compat` 显式蕴含前者和 common，R0 default 保旧签名。
- [ ] `[TEST]` 验证 no-default 只编 admin contract，client-adapter 可用，legacy default 全部旧 API 可编译。
- [ ] `[REV]` 源码 guard 证明 `core/` 无 client/common import，只有 `src/client_adapter/` 在 allowlist。
- [ ] 回滚点：旧 DefaultMQAdminExt facade 始终存在；feature optional 化若破坏 R0 即撤销并重新审计。

### PR-M07-06：Web/Tauri Dashboard 迁移

- [ ] `[DEV]` Dashboard 后端显式使用 admin-core `default-features = false, features = ["client-adapter"]`。
- [ ] `[DEV]` 管理查询、普通/事务测试发送、message/trace、NameServer 环境变更下沉 admin Session/facade。
- [ ] `[DEV]` page cache 使用 dashboard-owned QueueKey/admin QueueRef，不直接引入 model/protocol 以逃避 owner。
- [ ] `[TEST]` 对 Web/Tauri 逐项目执行最近 `AGENTS.md` 的 Rust/Node 构建；只有实际修改 dashboard-common 时，同时验证根 workspace 并条件验证 GPUI。
- [ ] `[REV]` 检查 UI/BFF 无完整 Client、MQAdminExt lifecycle、自建 producer/runtime，HTTP/API 行为保持。
- [ ] 回滚点：各 standalone 项目独立回滚；不能通过重新添加 Client 直接边恢复功能。

### PR-M07-07：Allowlist 与 consumer closeout

- [ ] `[DEV]` dependency/source guard 固定 workspace 两个目标位置和 standalone example；Proxy 现状在 M08 完成物理迁移前必须有带到期里程碑的临时 ledger。
- [ ] `[TEST]` 执行根 workspace、Example、Tauri、Web backend/frontend 的适用验证；若 dashboard-common 未变化，不运行或宣称 GPUI 迁移验证。
- [ ] `[REV]` 核对 manifest/source/normal closure，确保 common/remoting re-export 不隐藏 Client 直边。
- [ ] `[ARCH]` 发布给 M08 的 Proxy DTO/port/client runtime 清单。
- [ ] `[HUMAN]` 只在除 Proxy 待迁边外其余消费者已清零时批准进入 M08。

## 公共兼容面

- `rocketmq-rust` 旧 schedule/task/shutdown 路径和 admin-core 旧 DefaultMQAdminExt/MQAdminExt 签名在 R0/R1 保留。
- Admin legacy feature 必须完整编译旧 client/common 类型；下一 major 才删除。
- Dashboard 外部 HTTP/command payload 不因内部 adapter 迁移改变。
- NameServer/Broker wire、timeout 和路由回退语义保持；只改变依赖 owner。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-runtime
cargo test -p rocketmq-namesrv
cargo test -p rocketmq-broker
cargo test -p rocketmq-admin-core
cargo check -p rocketmq-mcp
cargo test -p rocketmq-mcp
cargo test -p rocketmq-mcp --all-features
cargo clippy --all-targets -p rocketmq-mcp --features streamable-http -- -D warnings
cargo doc -p rocketmq-mcp --no-deps
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
git diff --check
```

Standalone 项目在被触发时从各自目录执行：

```powershell
# rocketmq-example
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings

# Tauri src-tauri
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings

# Web backend
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features

# Tauri/Web frontend
npm ci
npm run build
```

每组命令从其项目根执行并单独记录，不从仓库根误跑。

只有实际修改 `rocketmq-dashboard-common` 时，才从 `rocketmq-dashboard/rocketmq-dashboard-gpui/` 条件执行：

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

### 本里程碑新增后执行

```powershell
cargo check -p rocketmq-admin-core --no-default-features
cargo check -p rocketmq-admin-core --no-default-features --features client-adapter
cargo check -p rocketmq-admin-core --features legacy-common-compat
python scripts/architecture_dependency_guard.py --mode baseline
python scripts/arc_mut_guard.py
```

## 回滚触发器

- Broker/NameServer/MCP/Dashboard 仍有或重新引入 Client direct/source 边。
- admin-core no-default 无法只编 contract，或 R0 legacy 签名无法编译。
- NameServer route timeout/回退、Broker send/pull/POP/transaction、Dashboard API 发生未批准变化。
- Legacy 能力迁入 runtime 后引入第二 runtime、detached work 或 ArcMut 增长。
- 任一 standalone consumer 未验证即宣称 allowlist 达标。

按项目独立回滚，并保持 admin/route/out-api adapter seam。若功能只能通过恢复禁止边才能工作，停止并升级 `[HUMAN]`，不得临时豁免后继续 M08。

## Exit Checklist

- [ ] `[REV]` 新 crate 无 `rocketmq-rust` 依赖，legacy ledger 只降不增。
- [ ] `[TEST]` MCP 默认/feature/doc contract 全绿且仍无副作用。
- [ ] `[REV]` Broker、NameServer manifest/source/normal closure无完整 Client。
- [ ] `[REV]` admin-core Client import 只在 `src/client_adapter/`，no-default/legacy feature 均符合设计。
- [ ] `[TEST]` 受影响 Dashboard 与 Example 按各自 AGENTS 完整验证。
- [ ] `[DEV]` source/metadata guard 报告除 M08 待迁 Proxy 外无临时例外。
- [ ] `[ARCH]` M08 的端口和 DTO 边界已冻结。
- [ ] `[HUMAN]` 所有临时例外有 owner、到期为 M08，批准进入 Proxy 拆分。

## 交接物

- 向 M08 交付中立 DTO/port、Proxy 临时 Client 边清单和 allowlist target rule。
- 向 M09 交付 legacy/admin/common compatibility ledger 和 standalone consumer 证据。
- 向 M11 交付 ServiceContext 注入、runtime drain 与剩余 ArcMut ledger。

## PR-M07-01 Legacy Runtime 排空 evidence

- [x] `[ARCH/OWNER]` schedule executor/scheduler/task/trigger、ScheduledTaskManager、ServiceManager、broadcast
  Shutdown 与 process signal handler 的 canonical owner 全部迁入 `rocketmq-runtime`；`rocketmq-rust` 的 schedule/task/shutdown
  文件只含精确 re-export，`wait_for_signal` 与 `service_manager!` 从 runtime 根重导出。ArcMut、WeakArcMut、
  SyncUnsafeCellWrapper、锁和队列均留在 legacy，没有借迁移扩大 runtime 职责。
- [x] `[DEPENDENCY/POLICY]` runtime 生产 manifest 保持零 RocketMQ 内部依赖；legacy 生产依赖收缩为 runtime、serde、tokio，
  tracing 仅供自带 example 的 dev-dependency。新增 `new-boundaries-no-legacy-runtime` 精确覆盖 10 个目标新边界 crate，
  禁止它们直接依赖 `rocketmq-rust`，且 target DAG 的 `rocketmq-runtime=[]` 保持不变。
- [x] `[CONSUMER]` root workspace 的 15 个 lifecycle consumer 文件已改用 runtime canonical path：schedule 4、service task 6、
  broadcast shutdown 2、process signal 3；standalone Example 的 8 个 signal 旧路径作为公开兼容面保留并由 contract 冻结。
- [x] `[COMPAT/ERROR]` 三项 Rust 差分测试证明 Shutdown、ScheduledTaskManager、ServiceManager 的 canonical/legacy 类型身份
  一致，并覆盖 signal delivery、schedule/cancel、service start/shutdown。owner API 统一返回 `RuntimeResult`，已有 Broker 领域签名
  使用 `BrokerAsyncTaskFailed` typed source 保留错误链；signal registration 新增 fallible API，旧 `wait_for_signal()` 继续返回 `()`
  并记录失败，不再 panic。既有 AsyncFnMut nightly gate 随 schedule owner 原样迁移，未新增第二种 gate 或行为。
- [x] `[TEST]` runtime 77 项、legacy 33 unit + 3 compatibility、Broker scheduled 1、Client scheduled 4、Store all-feature
  lib 484、M07-01 source contract 6 项通过；Broker/Client/Store/NameServer/Remoting all-feature compile 通过。
- [x] `[STANDALONE]` Example fmt/Clippy、Tauri backend fmt/all-feature Clippy、Web backend fmt/all-feature Clippy/all-target
  build 按最近 AGENTS 通过；共享依赖变化同步更新三份 lockfile。未修改 dashboard-common，因此未运行或宣称 GPUI 验证。
- [x] `[GOVERNANCE]` architecture baseline、35 项单测、6 个 violation fixtures、runtime enforcing audit、AGENTS routing、
  ArcMut 裸 guard/63 项单测/24 fixtures 通过。10 个既有 ArcMut/WeakArcMut import 因 canonical import 调整发生同 identity
  一对一 fingerprint relocation，按 ADR-013 批准后台账保持 1,170 identities/3,232 occurrences，零新增债务。
- [x] `[ERROR]` 首轮 typed-error guard 发现本包新增 FlowMonitor source stringification；改为直接返回 RuntimeResult，并将
  Broker 兼容签名改为 typed source 后复核只剩 main 既有 11 项（Broker 1、MCP 8、缺失治理文档 2）。该既有门禁失败未计为通过。
- [x] `[FINAL]` root workspace exact fmt、all-target/all-feature strict Clippy、M07-01 contract、architecture/ArcMut final guard、
  82 项 checklist 计数（41/41）与 `git diff --check` 通过；Clippy 仅输出不受 `-D warnings` 控制的 linker/future-incompat 提示。
- [x] `[COMPAT/ROLLBACK]` 独立
  [`10-legacy-runtime-compatibility-ledger.md`](10-legacy-runtime-compatibility-ledger.md) 冻结 owner、退出顺序、consumer、
  public path、nightly/error 约束与回滚层级。consumer 可先恢复 legacy import，canonical owner 保留；不得把 owner 代码、
  detached work 或危险 ArcMut escape 搬回 legacy。
- [x] `[INVENTORY]` PR-M07-01 父项关闭；82 个顶层工作包更新为 41 已完成、0 阻塞、41 未开始。M07 保持进行中，
  唯一下一工作包为 PR-M07-02 删除 MCP 冗余 Client 边。

## PR-M07-02 MCP 冗余 Client 边清理 evidence

- [x] `[DEPENDENCY/OWNER]` `rocketmq-mcp` manifest 与 lockfile 的 Client、common、remoting 三条未使用直接边已删除；
  `cargo tree -p rocketmq-mcp -e normal -i rocketmq-client-rust` 精确证明 Client 仅经
  `rocketmq-admin-core` 间接进入，MCP 的 direct normal tree 只保留自身实际 owner 依赖。
- [x] `[SOURCE/FACADE]` MCP `src/` 与 `tests/` 不含 `rocketmq_client(_rust)::`、`rocketmq_common::` 或
  `rocketmq_remoting::` 绕行；`AdminSession` 继续从 admin-core 的 Topic/Broker/Consumer/Cluster service 获取 capability，
  QueryFacade 的 session factory 与 start/shutdown 行为未改变。
- [x] `[CONTRACT/POLICY]` 新增 4 项 M07-02 contract，冻结 manifest、lockfile、source/facade 与 architecture baseline；
  Client root manifest consumer 基线从 5 收敛到 4，删除 MCP 的 Client/common/remoting 例外，target allowlist 继续排除 MCP。
- [x] `[TOOL/SECURITY]` 默认 catalog 精确保留 8 个只读/诊断 Tool；all-features catalog 精确新增 5 个
  change-planning Tool。新增 Rust 合同逐一构造 create topic、topic config、topic permission、broker config、consumer
  offset 五类计划，并证明全部 `mutates_cluster == false`、ephemeral、immutable；stdio integration 继续把 stdout 每行解析为
  JSON-RPC 协议帧，未出现诊断日志污染。
- [x] `[TEST/MCP]` `cargo check -p rocketmq-mcp`、default test（72 unit + 2 integration，1 个外部集群 E2E ignored）、
  all-features test（89 unit + 2 integration，1 ignored）、streamable-http all-target strict Clippy 与 no-deps Rustdoc 全部通过；
  tool/resource/prompt snapshot、2025-11-25 protocol、HTTP auth/security、sanitizer 与 read-only policy 均由同一矩阵覆盖。
- [x] `[GOVERNANCE]` architecture baseline、35 项单测、1 clean/6 violation fixtures、ArcMut guard、63 项单测、24 fixtures、
  AGENTS routing 与 Python compile 通过。未修改共享 public API、standalone manifest/lockfile 或 dashboard-common，因此不触发
  Example、Tauri、Web、GPUI 的消费者重验；没有 runtime ownership/source 变更，因此不触发 runtime audit。
- [x] `[ERROR]` error hygiene 仅复现 `main` 既有 11 项：Broker source stringification 1、MCP anyhow 8、缺失治理文档 2；
  本工作包没有新增 finding，故记录为 pre-existing failure，不计为通过。
- [x] `[FINAL/ROLLBACK]` root workspace exact fmt、all-target/all-feature strict Clippy 与 `git diff --check` 通过；Clippy 仅输出
  不受 `-D warnings` 控制的 linker/future-incompat 提示。回滚可恢复代码前的依赖解析，但不得重新加入 MCP 的 Client/common/
  remoting 直接边；若 admin-core capability 不足，必须先扩展 admin-owned port。
- [x] `[INVENTORY]` PR-M07-02 父项关闭；82 个顶层工作包更新为 42 已完成、0 阻塞、40 未开始。M07 保持进行中，
  唯一下一工作包为 PR-M07-03 NameServer RouteLookup 反转。
