# M07：Legacy Runtime 排空与 Client 依赖边收敛

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 进行中；PR-M07-01、PR-M07-02、PR-M07-03、PR-M07-04、PR-M07-05 已完成 |
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

- [x] `[ARCH]` 固定 `ClusterTestRouteLookup` 输入/输出、3 秒 deadline、缓存/回退和生命周期 owner。
- [x] `[DEV]` 默认 adapter 只依赖 protocol+transport，由 ServiceContext 拥有；移除 MQClientManager/admin registry。
- [x] `[TEST]` 对成功、timeout、NameServer 不可达、route 回退、shutdown 做差分。
- [x] `[REV]` 检查 NameServer manifest/source/normal closure 无 Client，deadline 不在每层重置。
- [x] 回滚点：旧 processor facade 保留；adapter 可切回旧实现，但 Client 边重新出现会阻塞 M07 Gate。

### PR-M07-04：Broker Client 边清零

- [x] `[DEV]` 远程结果改用 model；本地 transaction/POP 改用 store-api read outcome。
- [x] `[DEV]` 建 Broker-owned `BrokerPublishRoute` 和 out-api send/pull/result adapter；TopicPublishInfo 留 Client。
- [x] `[DEV]` 迁 query assignment 分配算法、route projection、escape bridge 和 test-only fixture。
- [x] `[TEST]` 覆盖 send/pull/pop/transaction/route/query-assignment 差分和 all-targets 编译。
- [x] `[REV]` 检查 Broker manifest/source/normal closure无 Client/client-api，processor 不依赖 store facade。
- [x] 回滚点：按 result/route/assignment/read 投影切片回滚；不得新增 client-api 或 admin-core 绕行。

### PR-M07-05：Admin contract 与 Client adapter 收口

- [x] `[ARCH]` 按 Topic/Broker/Consumer/Security/Lite 拆 admin-owned capability/request/result，冻结 R0 legacy surface。
- [x] `[DEV]` 建 `core/` 与 `client_adapter/` 同领域模块；业务 MQAdminExt/producer 编排进入 adapter，路径稳定的旧 Default facade 仅作 R0 compatibility seam。
- [x] `[DEV]` 注入 admin-owned Clock 替代 TimeUtils；static-topic 纯 planner 留 core，SDK 调用进 adapter，文件 I/O 留 CLI。
- [x] `[DEV]` 实现 `client-adapter` optional feature；`legacy-common-compat` 显式蕴含前者和 common，R0 default 保旧签名。
- [x] `[TEST]` 验证 no-default 只编 admin contract，client-adapter 可用，legacy default 全部旧 API 可编译。
- [x] `[REV]` 源码 guard 证明 `core/` 无 client/common import，只有 `src/client_adapter/` 在 allowlist。
- [x] 回滚点：旧 DefaultMQAdminExt facade 始终存在；feature optional 化若破坏 R0 即撤销并重新审计。

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

## PR-M07-03 NameServer RouteLookup 反转 evidence

- [x] `[BOUNDARY/OWNER]` `ClusterTestRouteLookup` 已改为注入式异步 port；默认
  `TransportClusterTestRouteLookup` 由 `ServiceContext` 的 `namesrv.cluster-test-route-lookup` 子上下文拥有，内部仅用 canonical
  protocol header/command 与 `TransportClient`。启动不访问外部环境，shutdown 会取消活动解析并等待 task tree；未创建第二套
  `RuntimeOwner`、MQClientManager、DefaultMQAdminExt 或 admin registry。
- [x] `[COMPAT/DEADLINE]` `productEnvName` 继续使用既有 top-addressing URL 规则，解析后的 SocketAddr 列表按 lookup 缓存；连接失败后
  清缓存以便下次重新解析。本地 route 仍优先并附加 order config，缺失时才调用注入 lookup。每次 lookup 只创建一个 3 秒绝对
  `ShutdownDeadline`，同一值贯穿 HTTP addressing、DNS 和全部 transport 尝试，不在分层或重试时重置。
- [x] `[DEPENDENCY/POLICY]` NameServer manifest 与 lockfile 删除 `rocketmq-client-rust`，新增 protocol+transport owner 边；`src/`
  无 `rocketmq_client(_rust)::`。architecture baseline 删除 NameServer 的 manifest/source Client 例外，Client root manifest consumer
  基线从 4 收敛到 3；5 项 M07-03 contract 冻结 manifest/lockfile/source、deadline、ServiceContext owner 与 baseline。
- [x] `[TEST/NAMESRV]` transport adapter 的成功解码/端点缓存、绝对 timeout、不可达端点和活动解析 shutdown 4 项测试通过；processor
  route fallback、cluster-test boot 与 remoting 集成通过。`cargo test -p rocketmq-namesrv` 完成 179 unit、1 binary、7
  CheetahString、6 remoting integration、2 size、4 index 与 8 doc tests（1 个既有 ignored），无失败。
- [x] `[GOVERNANCE]` architecture baseline guard、35 项单测、1 clean/6 violation fixtures、新旧 M07 contract 共 9 项通过；runtime
  enforcing audit、AGENTS routing 通过。ArcMut 默认 admin lookup 的 3 identities/6 occurrences 被删除，3 个保留 occurrence 按
  ADR-013 一对一 relocation，台账由 1,170/3,232 收敛到 1,167 identities/3,226 occurrences；63 项 guard 单测与 24 fixtures 通过。
- [x] `[ERROR]` error hygiene 仅复现 `main` 既有 11 项：Broker source stringification 1、MCP anyhow 8、缺失治理文档 2；
  NameServer 本次新增的 typed network/RPC/config errors 没有产生 finding，故该命令仍记录为 pre-existing failure，不计为通过。
- [x] `[FINAL/ROLLBACK]` NameServer all-target/all-feature strict Clippy、package fmt 与全部测试通过；回滚可替换注入 adapter，但
  processor facade、protocol contract、单 deadline 和 ServiceContext owner 必须保留，不得恢复 Client/admin registry 直接边。
- [x] `[INVENTORY]` PR-M07-03 父项关闭；82 个顶层工作包更新为 43 已完成、0 阻塞、39 未开始。剩余分布为 M07 4、M08 6、
  M09 6、M10 5、M11 12、M12 6；M07 保持进行中，唯一下一工作包为 PR-M07-04 Broker Client 边清零。

## PR-M07-04 Broker Client 边清零 evidence

- [x] `[RESULT/READ OWNER]` Broker 远程 send/pull 统一返回 `rocketmq-model` 的 `SendResult`/`PullOutcome`；新增
  `rocketmq-store-api::ReadOutcome<T>` 作为拥有型本地读取结果。transaction 与 POP 仅在 Broker-local store adapter 内接触
  legacy `GetMessageResult`，解码后传递 `MessageExt` 所有权，不再借用 Client `PullResult` 或为记录补造 `ArcMut`。
- [x] `[ROUTE/ADAPTER OWNER]` Broker-owned `BrokerPublishRoute` 独立承担 ordered/static/normal route projection、轮询与 broker
  避让；out-api 的 request、raw response、send/pull 映射拆入 Broker 私有 adapter。Client 的 `TopicPublishInfo` 未搬入共享层，
  escape bridge、lite sharding、runtime fixture 和 route cache 全部改用 Broker/model owner。
- [x] `[ASSIGNMENT/DEPENDENCY]` query assignment 直接使用 model allocation contract/算法。Broker manifest、lockfile、源码外部
  Client API 和 `cargo tree -p rocketmq-broker -e normal` 中的 `rocketmq-client-rust` 匹配均为 0；architecture baseline 删除 Broker
  manifest 与 9 个 source 例外，workspace Client manifest consumer 基线由 3 收敛到 2。
- [x] `[TEST/BEHAVIOR]` store-api 19 项、out-api 8 项、query assignment 10 项、transaction 6 项、POP revive 13 项、route 2 项和
  RocksDB POP consumer 4 项聚焦测试通过；Broker default/all-feature all-targets 编译、Broker all-feature strict Clippy 与 RocksDB
  feature strict Clippy 通过。`cargo test -p rocketmq-broker` 串行复核为 504 passed、25 failed、1 ignored；25 项是可在本包外独立
  复现的既有 Broker 测试失败（以 Lite 配置键不匹配及既有 controller/lifecycle 用例为主），因此未把全包测试记录为通过。
- [x] `[GOVERNANCE]` M07 Client-edge 合同 15 项、architecture baseline guard、1 clean/6 violation fixtures、architecture/ArcMut
  guard 单测合计 98 项与 ArcMut 24 fixtures 通过；ArcMut 台账净减少 4 identities/10 occurrences，由 1,167/3,226 收敛到
  1,163/3,216，9 个保留出现点按 ADR-013 一对一 relocation 批准，零新增共享可变债务。
- [x] `[FINAL]` root workspace exact fmt 与 all-target/all-feature strict Clippy、AGENTS routing、`git diff --check` 通过；Clippy
  仅输出不受 `-D warnings` 控制的 linker/future-incompat 提示。error architecture guard 仍只复现 main 既有 11 项（Broker 1、
  MCP 8、缺失治理文档 2），该既有失败未计为通过。
- [x] `[TARGET GAP]` target guard 仍为 incomplete：缺少 `rocketmq-proxy-core`、`rocketmq-proxy-cluster`、
  `rocketmq-proxy-local` 3 个计划 crate，并有 153 项目标态差距（Client source 96、Client manifest 3、目标 DAG 直接边 52、
  传递闭包边 2）。Client source 差距分布为 Proxy 36、Admin Core 35、Tauri backend 25、Web backend 3，分别交给
  PR-M07-05、PR-M07-06、M08 与 M09 收口；Broker 不在剩余 Client 差距中。
- [x] `[ROLLBACK/INVENTORY]` 回滚必须按 read outcome、out-api adapter、route、assignment 四个投影切片执行，保留 model/store-api
  owner，且不得恢复 Broker Client 边或新增 client-api/admin-core 绕行。父项关闭后 82 个顶层工作包为 44 已完成、0 阻塞、
  38 未开始；剩余分布为 M07 3、M08 6、M09 6、M10 5、M11 12、M12 6，唯一下一工作包为 PR-M07-05。

## PR-M07-05 Admin contract 与 Client adapter 收口 evidence

- [x] `[CONTRACT/OWNER]` Topic、Broker、Consumer、Security、Lite 五组 capability/request/result、`AdminError`、`AdminResult`
  与 object-safe `Clock` 归 `core/`；`--no-default-features` 的 direct normal tree 只含 model/protocol/security-api 与通用库，
  不含 Client/common/remoting/rocketmq-rust/rocketmq-error。
- [x] `[FEATURE/ADAPTER]` `default = ["legacy-common-compat"]`；`client-adapter` 只激活 Client；legacy 显式蕴含
  client-adapter 与 common。旧领域 service、MQAdminExt/producer 编排和 SDK 类型迁入 `src/client_adapter/legacy/`，现代五组
  capability 由 `AdminSession` 实现；路径稳定的 `src/admin/default_mq_admin_ext.rs` 保留为 R0 facade，并只通过 adapter alias
  接触 Client。
- [x] `[CLOCK/STATIC-TOPIC/CLI]` 新 planner 对 broker 排序去重、按队列 round-robin，单次采样 Clock；新建映射 epoch 为
  `now/now+1000`，扩容为 `max(max_existing+1000, now)`，队列缩减和 epoch 溢出返回 typed error。static-topic JSON 文件读写、
  临时目录、`.before/.after` 文件名与 `.bak` 兼容语义已归 CLI 并由聚焦测试冻结。
- [x] `[TEST/CONSUMER]` Admin Core no-default、client-adapter 与 legacy default 三套 test/strict Clippy 通过；default 105 unit
  及全部 integration 通过。MCP 显式使用 `default-features = false, features = ["client-adapter"]`，default 72 unit + 2
  integration、all-feature 89 unit + 2 integration、streamable-http strict Clippy 与 no-deps Rustdoc 通过；CLI 文件测试与
  TUI check 通过。
- [x] `[STANDALONE]` Example、Tauri backend、Web backend 按最近 AGENTS 完成 fmt/strict Clippy；Web backend 额外完成
  all-target/all-feature build。定向清理独立 target 的旧 Client/Admin rmeta 后重建通过；未修改 dashboard-common，未触发 GPUI。
- [x] `[GOVERNANCE/ERROR]` architecture baseline、119 项相关治理测试、runtime enforcing audit、ArcMut guard 与 24 fixtures
  通过。Default facade 的既有 ArcMut import 按 ADR-013 一对一 relocation；message/CLI/TUI 删除 8 identities/9 occurrences，
  台账由 1,163/3,216 降至 1,155/3,207。Error guard 只保留 main 既有 11 项（Broker 1、MCP 8、治理文档 2），未计为通过。
- [x] `[TARGET GAP/INVENTORY]` target guard 差距由 153 降至 115：Client source 61（Proxy 35、Tauri 24、Web 2）、
  Client manifest 3、目标 DAG 直接边 49、传递闭包 2；Admin Core 与 Broker 均退出 Client source/违规 DAG 清单。父项关闭后
  82 个顶层工作包为 45 已完成、0 阻塞、37 未开始；剩余 M07 2、M08 6、M09 6、M10 5、M11 12、M12 6，唯一下一工作包
  为 PR-M07-06 Dashboard 迁移。
