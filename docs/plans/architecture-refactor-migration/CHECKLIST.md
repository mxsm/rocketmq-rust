# RocketMQ Rust 架构重构执行与完成 Checklist

> 设计依据：[`docs/architecture-refactor-design.md`](../../architecture-refactor-design.md)
> 实施手册：[`README.md`](README.md)
> 用途：跟踪 4 个 Phase、12 个里程碑和每次 PR/交付的完成证据
> 规则：只有证据可复现、审查已签署且回滚路径有效时，才能把 `[ ]` 改为 `[x]`

## 1. 填写约定

- 状态只使用：`未开始`、`进行中`、`阻塞`、`待验收`、`已完成`。
- 每个里程碑的详细范围、PR 步骤、命令和 Exit Checklist 以对应任务文档为准；本文件不替代任务文档。
- 每完成一个 PR，复制“每次交付完成记录模板”到本文件末尾或当期交付记录中，填写实际命令、退出码和证据路径。
- 运行期证据放在 `target/architecture-refactor/Mxx/<run-id>/`，不提交 Git；可重复 fixture/golden 按所属 crate 约定提交。
- 任何未执行、失败或仅计划未来提供的命令都必须明确记录，不能勾选为通过。
- `[HUMAN]`、`[ARCH]`、`[DEV]`、`[REV]`、`[TEST]` 的职责定义见实施手册；签署人不得代替其他角色勾选。

## 2. 总体进度

| Phase | 里程碑 | 状态 | 负责人 | 计划窗口 | 完成日期 | Evidence Index |
|---|---|---|---|---|---|---|
| Phase 1 | M01–M03 | 已完成 | Codex 多代理执行组 | 6–8 周 | 2026-07-11 | [`PHASE-1-DELIVERY.md`](phase-1-safety-foundation/PHASE-1-DELIVERY.md) |
| Phase 2 | M04–M09 | 已完成 | Codex 执行组 | 12–16 周 | 2026-07-18 | [`09-phase-2-gate-evidence.md`](phase-2-core-boundaries/09-phase-2-gate-evidence.md) |
| Phase 3 | M10–M11 | 进行中 | Codex 执行组 | 8–12 周 | — | [`phase-3-production-readiness/`](phase-3-production-readiness/) |
| Phase 4 | M12 | 未开始 | 待分配 | 8–12 周 | — | — |

### 2.1 剩余重构盘点（2026-07-19）

> 统计口径：只统计 82 个顶层 `PR-Mxx-yy` 工作包；M06-03a～ah 等内部迁移证据不重复计数。

| 指标 | 已完成 | 进行中 | 未开始/未完成 | 目标 |
|---|---:|---:|---:|---:|
| PR 级工作包 | 75 | 1（PR-M11-12） | 6（PR-M12-01～06）；合计 7 尚未完成 | 82 |
| 里程碑 | 9（M01–M09） | 2（M10 待验收、M11 实施中） | 1（M12） | 12 |
| 新增边界 crate | 10 | 0 | 0 | 10 |
| 根 workspace package | 32 | — | 0 | 32 |
| Phase Gate | 2 | 1（Phase 3） | 1（Phase 4） | 4 |

剩余 7 个工作包分布：M10 为 0 个、M11 为 1 个且正在实施、M12 为 6 个且尚未开始。
PR-M10-05 已完成性能门禁实现；真实固定硬件 baseline/candidate 与 HUMAN M10 Gate 尚未完成，因此 M10 为
`待验收`而非`已完成`。M11 为`实施中`，当前下一工作包为 PR-M11-12。

PR-M11-12 的内部子切片不重复计入 82 个顶层工作包。Issue #8385 的 M11-12as 子切片完成后，当前 ArcMut reviewed
baseline 为 473 identities / 1,227 occurrences，其中 production 为 294/725、test 为 165/462、compatibility
为 14/40。production 剩余分布和完成目标如下：

| owner | identity / occurrence | PR-M11-12 完成目标 |
|---|---:|---|
| Client | 0 / 0 | 已完成 DefaultMQProducer facade/implementation/registry 标准 Arc/Weak、配置快照、生命周期/任务接纳边界，并拆除强引用环 |
| Broker | 172 / 417 | Topic route/queue mapping、TopicConfig、POP buffer/checkpoint 与 POP/POP Lite processor/long-poll lifecycle owner 已完成；继续完成 BrokerRuntime、offset、schedule/其他 processor/transaction owner 安全化 |
| Store | 122 / 308 | TopicConfig 只读代际 carrier 已完成；继续完成 message store、CommitLog/Flush、queue、Rocks/Timer 与 HA owner/actor 安全化 |

ArcMut production/public compatibility 清零之后，PR-M11-12 还必须在同一冻结候选快照完成 stable feature matrix、
Miri/Loom 可用切片、soak/SLO fault、dashboard/runbook/rollback 证据；M10 固定硬件性能、五镜像动态验证、
Kind/K3d 七场景与 `[ARCH]`/`[REV]`/`[TEST]`/`[HUMAN]` 签署是验收 Gate，不额外增加顶层工作包数量。

目标态依赖债务不能与工作包计数混用：`architecture_dependency_guard.py --mode target` 当前严格通过，
表示未登记的目标 DAG finding 为 0；它不表示 R0 兼容依赖已经物理删除。现存边分为 35 条精确
compatibility/composition ledger 和 3 条 dev-only 测试边，caller/target/kind/path/alias 任一扩张都会失败。

兼容债务退出窗口如下；这里是依赖边数量，不是剩余工作包数量：

| 退出窗口 | 边数 | 后续收口重点 |
|---|---:|---|
| R1 | 29 | workspace 内 Common/Remoting/legacy runtime 消费方迁移 |
| next-major | 4 | standalone Example 与 Common Protocol public re-export 删除 |
| long-term | 2 | 已批准的 Broker/Store 与 store-inspect/Store composition |

M09-01 已物理清除 `rocketmq-tieredstore → rocketmq-common` 直接边及其两条传递差距；
M09-02 又物理清除 Filter/Common、Remoting/Macros、store-inspect/Common 三条边，并把 8 条经 purity
审查确认的必要 composition/error/lifecycle 边纳入目标 DAG，11 条到期输入全部归零，ledger 49 → 38。
M09-04 再删除 MCP 未使用的 Auth/Error direct edges，并把承担 owned task 与 BlockingExecutor 的 Runtime 边
纳入目标 DAG；M09-04 到期输入 3 → 0，总 ledger 38 → 35，Client allowlist 固定为 workspace 2 + standalone 1。
`rocketmq-proxy-core`、`rocketmq-proxy-cluster`、`rocketmq-proxy-local` 和 Client 临时账本继续保持零。
后续工作必须让上述 ledger 只降不增，并按窗口完成迁移或形成明确的长期架构批准。

额外治理风险：默认 ArcMut baseline guard、24 fixtures 与 65 项 guard 单测均通过，但显式以 M09 检查时，
现有 baseline 的 `current_milestone` 仍为 M05，并报告 821 个 M05/M06/M08 过期条目。本次未新增 ArcMut，
也未篡改 baseline 消除告警；该治理漂移不计入上述顶层工作包统计，必须在后续 ArcMut/R1/M11 收口时纠正。

## 3. Phase 1：安全性与基础治理

### M01 治理、依赖策略与可重复基线

任务文档：[`01-governance-and-baselines.md`](phase-1-safety-foundation/01-governance-and-baselines.md)

- [x] PR-M01-01：冻结仓库事实和验证路由
- [x] PR-M01-02：实现依赖 policy 与 guard
- [x] PR-M01-03：实现 ArcMut usage guard
- [x] PR-M01-04：建立兼容与性能基线索引
- [x] PR-M01-05：接入 CI、AGENTS 路由和例外流程
- [x] 对应任务文档的 Exit Checklist 全部通过
- [x] M01 evidence index、回滚点和 M02/M03 交接物已归档

### M02 P0 正确性、请求生命周期与关闭时序

任务文档：[`02-correctness-and-lifecycle.md`](phase-1-safety-foundation/02-correctness-and-lifecycle.md)

- [x] PR-M02-01：锁定并修复 Flush 失败回归
- [x] PR-M02-02：实现 TaskGroup 动态 child lease
- [x] PR-M02-03：实现 PendingRequestGuard 与 complete-once
- [x] PR-M02-04：统一绝对 ShutdownDeadline
- [x] PR-M02-05：清理首批 ArcMut 与 controller downcast
- [x] 对应任务文档的 Exit Checklist 全部通过
- [x] M02 evidence index、回滚点和 M05/M06/M07 交接物已归档

### M03 基础合同、Client 中立类型与安全预检

任务文档：[`03-foundation-contracts.md`](phase-1-safety-foundation/03-foundation-contracts.md)

- [x] PR-M03-01：创建最小 `rocketmq-model`
- [x] PR-M03-02：迁移中立结果与分配算法
- [x] PR-M03-03：创建 `rocketmq-security-api`
- [x] PR-M03-04：解除 observability 对 common 的依赖
- [x] PR-M03-05：执行 secure profile dry-run
- [x] PR-M03-06：完成 Phase 1 foundation 收口
- [x] 对应任务文档的 Exit Checklist 全部通过
- [x] M03 evidence index、回滚点和 M04/M06/M11 交接物已归档

### Phase 1 Gate

- [x] P0 正确性回归全部通过
- [x] ArcMut 新增为零，首批迁移切片数量下降
- [x] runtime/task/pending/shutdown 生命周期证据可重复
- [x] observability 依赖闭包不含 facade/legacy
- [x] foundation crate 禁边和 Client 中立类型兼容测试通过
- [x] `[ARCH]` 已签署（架构审查代理，2026-07-11）
- [x] `[REV]` 已签署（独立代码审查代理，2026-07-11）
- [x] `[TEST]` 已签署（根工作区与独立消费者验证，2026-07-11）
- [x] `[HUMAN]` 已批准进入 Phase 2（用户授权按 Phase 自动交付、合并并继续，2026-07-11）

## 4. Phase 2：核心边界与 API 收敛

### M04 Protocol 提取

任务文档：[`04-protocol-extraction.md`](phase-2-core-boundaries/04-protocol-extraction.md)

- [x] PR-M04-01：创建 crate 与单 request-code spike
- [x] PR-M04-02：迁移 model 前置的 wire primitives
- [x] PR-M04-03：分批迁移声明式 schema
- [x] PR-M04-04：拆分 static-topic、route 与 RPC 纯逻辑
- [x] PR-M04-05：迁移 Trace record 与 message codec
- [x] PR-M04-06：完成 feature/facade 与全量收口
- [x] 对应任务文档的 Exit Checklist 全部通过

### M05 Transport 提取

任务文档：[`05-transport-extraction.md`](phase-2-core-boundaries/05-transport-extraction.md)

- [x] PR-M05-01：创建 crate 与单 request lifecycle spike
- [x] PR-M05-02：迁移 codec、buffer 与 net primitives
- [x] PR-M05-03：迁移 client、RPC runtime 与 pending table
- [x] PR-M05-04：迁移 server、processor adapter 与 shutdown
- [x] PR-M05-05：实现有界 admission 与 security adapter
- [x] PR-M05-06：完成 remoting facade、feature 与 V2 决策
- [x] 对应任务文档的 Exit Checklist 全部通过

### M06 Store API、Local 与 RocksDB 边界提取

任务文档：[`06-storage-boundary-extraction.md`](phase-2-core-boundaries/06-storage-boundary-extraction.md)

- [x] PR-M06-01：完成 Store capability spike
- [x] PR-M06-02：迁移中立 receipt/read result 与 compatibility bridge
- [x] PR-M06-03：创建 Local crate 并迁移 CommitLog/load/recovery
  - [x] M06-03a：创建 Local leaf foundation 并迁移六个纯 MappedFile leaf
  - [x] M06-03b：迁移 CommitLog load/recovery 中立规划值与纯 planner
  - [x] M06-03c：迁移 mapped-file progress 与 reference lifecycle kernel
  - [x] M06-03d：迁移 OS file/path/offset 与 open/resize/preallocate/rename/delete kernel
  - [x] M06-03e：迁移泛型 mmap 初始化生命周期与 lazy statistics kernel
  - [x] M06-03f：迁移 CommitLog V1/blank record 常量、blank helper 与静态 frame cursor
  - [x] M06-03g：迁移有界 CommitLog record parser，并完成 fail-closed、无堆分配与完整 dispatch golden 审查
  - [x] M06-03h：迁移 normal recovery 双水位纯状态机，并接入 standard/optimized 两路径
  - [x] M06-03i：迁移 abnormal recovery 三水位纯状态机，并接入 standard/optimized、dup/controller 门控
  - [x] M06-03j：迁移 CommitLog 文件元数据与纯长度校验，并接入 parallel/sequential loader
  - [x] M06-03k：迁移 CommitLog 映射计划与 mmap advice/prefetch 统计归约内核
  - [x] M06-03l：迁移 CommitLog recovery memory hint 平台执行到 Local
  - [x] M06-03m：迁移 CommitLog filesystem metadata 收集、校验与空尾删除到 Local
  - [x] M06-03n：迁移 CommitLog 目录发现、文件过滤与稳定文件名排序到 Local
  - [x] M06-03o：迁移 CommitLog append 值与最小 mapped-file config 到 Local
  - [x] M06-03p：迁移 MemoryLockManager 与 mlock/munlock 平台边界到 Local
  - [x] M06-03q：迁移 TransientStorePool 到 Local 并保留 Store 公开类型身份
  - [x] M06-03r：迁移 MappedFile 固定页阈值判定到 Local 并保留 Store 常量路径
  - [x] M06-03s：迁移 MappedFile 内存锁范围裁剪策略到 Local，保留 Store 指针适配并冻结全 production 角色/dataflow 防回流契约
  - [x] M06-03t：迁移 MappedFile warmup 调度策略到 Local，保留 Store 实际 I/O、错误与可观测性适配
  - [x] M06-03u：迁移 normal recovery 文件扫描窗口 planner 到 Local，保留 Store recovery 编排、日志与状态机适配
  - [x] M06-03v：迁移 recovery ConsumeQueue truncate 纯判定到 Local，四条 Store recovery 路径直接共享同一策略
  - [x] M06-03w：迁移 abnormal recovery confirm-candidate checked calculation 到 Local，保留 Store 两条 raw-input adapter
  - [x] M06-03x：迁移 CommitLog active memory-lock target 纯 planner 到 Local，保留 Store config 与锁生命周期适配
  - [x] M06-03y：迁移 MappedFile cache-residency 纯范围校验到 Local，保留 Store 三平台探测与指标适配
  - [x] M06-03z：迁移 Linux MappedFile cache-residency 纯整数页规划到 Local，保留 Store 平台探测与指针适配
  - [x] M06-03aa：迁移 CommitLogLoader 完整编排到 Local，Store 保留薄 wrapper 与函数表 target adapter
  - [x] M06-03ab：迁移 DefaultMappedFile raw byte/progress owner 到 Local，Store 保留 mmap/lifecycle/platform adapter
  - [x] M06-03ac：迁移 CommitLog append frame finalization/segment-roll/blank marker kernel 到 Local，Store 保留业务、CRC 与 I/O adapter
  - [x] M06-03ad：迁移 CommitLog append TOTALSIZE 解码与 batch frame traversal owner 到 Local，Store 保留业务、CRC、context、计时/result 与 MappedFile I/O adapter
  - [x] M06-03ae：迁移 CommitLog fixed-header layout 与 Store timestamp probe owner 到 Local，Store recovery/pickup 仅保留 MappedFile、checkpoint 与范围 adapter
  - [x] M06-03af0：修复 CommitLog EOF retry encoded buffer ownership，三条 callback 在 EndOfFile 前归还原 BytesMut
  - [x] M06-03af：迁移 CommitLog bounded append-attempt 编排到 Local，Store 保留 mapped-file、active-lock、计时/result 与后处理 adapter
  - [x] M06-03ag：迁移 CommitLog standard recovery declared-frame read owner 到 Local，Store normal/abnormal 各保留一条 MappedFile exact-read adapter
  - [x] M06-03ah：迁移 CommitLog normal recovery 单 segment record-loop 编排到 Local，Store standard/optimized 仅保留 MappedFile、解析、dispatch、日志与统计 adapter
  - [x] M06-03（MappedFile owner）：迁移 `MappedFile` trait、`DefaultMappedFile`、mapping backend、select result 与平台 FFI 到 Local；Store 仅保留 append/mmap compatibility adapter
  - [x] M06-03（native mmap/load owner）：迁移 `NativeMappedMemory`、zero-copy region 与 native `CommitLogLoader` adapter 到 Local；Store loader 收敛为四条精确 re-export
  - [x] M06-03ai：迁移 abnormal recovery 单 segment record-loop 编排到 Local，Store standard/optimized 仅保留 MappedFile、解析、dispatch、日志与统计 adapter
  - [x] M06-03aj：迁移 CommitLog safe/optimized/sequential load 外层决策与 fallback 顺序到 Local，Store 仅保留两个 load adapter 与 legacy 日志
  - [x] M06-03ak：迁移 CommitLog optimized/standard recovery route 与环境值语义到 Local，Store normal/abnormal 各保留两个 async adapter
  - [x] M06-03al：迁移 CommitLog put-message lock 统计/快照与 active memory-lock 当前区域状态到 Local，Store 保留平台 lock/unlock adapter
  - [x] M06-03am：将 CommitLog confirm/put-lock/begin-lock/active-lock/load-statistics 组合状态收敛为 Local `CommitLogRuntimeState`，Store 根结构只持一个 Local runtime-state owner
  - [x] M06-03an：迁移 MappedFileQueue flushed/committed/store-timestamp 原子进度与 commit 串行锁到 Local，Store 保留文件集合与 I/O 编排
  - [x] M06-03ao：迁移 MappedFileQueue path/segment-size/collection 组合 owner 到 Local 泛型 storage，Store 注入 ArcSwap 后端并保留算法/allocate adapter
  - [x] M06-03ap：迁移 MappedFileQueue 相邻连续性、范围窗口、按时间与按 offset 索引算法到 Local，Store 保留 ArcSwap 快照、日志与对象适配
  - [x] M06-03aq：迁移 MappedFileQueue 80% 预分配与空/满 segment roll 纯决策到 Local，Store 保留两阶段状态观察及 allocate/create 副作用
  - [x] M06-03ar：迁移 AllocateMappedFileService request path/size 身份、offset 解析、Display/Eq 与优先级排序到 Local key，Store request 只保留完成通知和结果状态
  - [x] M06-03as：迁移 AllocateMappedFileService 预热配置/阈值与 TransientStorePool fast-fail 容量决策到 Local，Store 只采集 MessageStoreConfig 和 queue/pool 快照
  - [x] M06-03at：迁移 MappedFileQueue dirty-tail truncate 与 recovery reset 纯规划到 Local，Store 保留 position/destroy/ArcSwap 副作用及两快照观察顺序
  - [x] M06-03au：将 AllocateMappedFileService worker/request table/queue、通知、timeout、TransientStorePool 与 mapped-file create owner 整体迁到 Local，Store 仅保留 MessageStoreConfig 投影与精确 re-export
  - [x] M06-03av：迁移 MappedFileQueue 目录发现、排序/校验、尾部空文件清理、加载初始化与 service/sync create I/O owner 到 Local，Store 仅应用 load outcome 与 ArcSwap collection
  - [x] M06-03aw：迁移 MappedFileQueue delete/retry-delete、swap/clean、shutdown/destroy lifecycle owner 到 Local，Store 仅保留 check/time-source 与 ArcSwap collection 应用
  - [x] M06-03ax：迁移 MappedFileQueue warmup/lazy-mmap 聚合及 max/min/total/available/fall-behind/roll 查询 owner 到 Local，Store 精确 re-export stats 并保留 snapshot adapter
  - [x] M06-03ay：迁移 CommitLog 根 owner 到 Local 泛型 `CommitLogRoot`，Store 旧 `CommitLog` 收敛为单字段 facade 与 composition adapter
  - [x] M06-03az：迁移 CommitLog append outcome resolution owner 到 Local，Store 仅保留 status/log/lock/flush/HA adapter
  - [x] M06-03ba：迁移 CommitLog recovery completion owner 到 Local，Store 四条 recovery 路径统一为 completion side-effect adapter
  - [x] M06-03bb：冻结 Local/Store compatibility ledger、feature/re-export/facade contract 并完成父项收口
- [x] PR-M06-04：机械迁移 Flush 与 Group Commit
  - [x] M06-04a：迁移 GroupCommit request、batch completion 与 SyncFlush runtime stats owner 到 Local，Store 保留 status/error/health adapter
  - [x] M06-04b：迁移 canonical `FlushProgress` 与 MappedFileQueue flush/commit I/O owner，Store 保留 legacy path
  - [x] M06-04c：迁移 GroupCommit worker 驱动与 checkpoint completion owner
  - [x] M06-04d：迁移 AsyncFlush/CommitRealTime worker 驱动与生命周期 owner
  - [x] M06-04e：收敛 FlushManager facade、SyncFlush/ack adapter、兼容 ledger 与父项验收
- [x] PR-M06-05：迁移 CQ 与 Index
  - [x] M06-05a：迁移 canonical 20B CQ record codec 与边界校验到 Local
  - [x] M06-05b：迁移 SingleConsumeQueue scan/search/recovery kernel 到 Local
  - [x] M06-05c：迁移 BatchConsumeQueue 与 CQExt storage kernel 到 Local
  - [x] M06-05d：迁移 ConsumeQueue root/store/dispatch owner，Store 保留 composition adapter
  - [x] M06-05e：迁移 40B IndexHeader 与 20B index entry/slot codec 到 Local
  - [x] M06-05f：迁移 IndexFile put/query driver 到 Local
  - [x] M06-05g：迁移 IndexService lifecycle/query/dispatch root，冻结 ledger 并完成父项验收
- [x] PR-M06-06：迁移 HA、Replication 与 Transfer
  - [x] 冻结 HA wire、replica offset/ack、leader/follower progress 与 transfer partial-write 契约
  - [x] 迁移 transfer planner/segment/engine/metrics、flow-control 与 replication state root 到 Local
  - [x] Store 仅保留 socket、Remoting/controller DTO、CommitLog/LocalFileMessageStore 与 lifecycle adapter
  - [x] focused HA/transfer、Store all-feature lib、runtime audit、architecture/ArcMut guard 全部通过
  - [x] 冻结兼容 ledger、回滚点与 34/48 顶层工作包盘点
- [x] PR-M06-07：迁移 Timer、POP 与 Local Services
  - [x] Timer log/wheel/slot、checkpoint state/codec、metrics 与 schedule/recovery/backlog/TPS kernel 迁入 Local
  - [x] POP ACK/BatchACK/Checkpoint、filter、cold-data service 与 stats state 迁入 Local，Store 旧路径保持兼容
  - [x] Store Timer 保留 MessageExt/CommitLog/CQ/config/DataVersion/file I/O adapter，stats 保留 Broker/TaskGroup lifecycle adapter
  - [x] Local hook registry 接入真实 put-message hook 路径，注册顺序和 legacy getter 保持不变
  - [x] Timer/POP/重启恢复/服务生命周期、Store 全量、runtime/architecture/Clippy 门禁通过并冻结 35/47 盘点
- [x] PR-M06-08：收敛 LocalFileMessageStore facade、composition 与 config
  - [x] Local canonical 拥有 lifecycle/query/reput/cleanup policy 与 `LocalStoreComposition` root
  - [x] legacy `MessageStoreConfig` 保持 Serde/default/alias envelope，并投影 immutable `LocalBackendConfig`
  - [x] Store 公共 `LocalFileMessageStore` 路径不变，仅连接 Broker/CommitLog/CQ/runtime effect adapter
  - [x] public-path doctest、config、lifecycle/query/reput/cleanup、Store/Local 回归与架构门禁通过
  - [x] 兼容 ledger、回滚点与 36/46 顶层工作包盘点已冻结
- [x] PR-M06-09：创建 RocksDB foundation
  - [x] 新增 `rocketmq-store-rocksdb`，`default = []`，独占 native `rocksdb` 依赖
  - [x] config/CF/key-value/codec/store/snapshot、CQ/Index kernel、maintenance/runtime 迁入新 owner
  - [x] Store 旧深路径保持精确 re-export，仅保留 config source、DispatchRequest 与 CommitLog dispatcher adapter
  - [x] Store default/no-default 与 Local dependency tree 均无 native RocksDB；Rocks owner tree 独立包含 native 库
  - [x] foundation/snapshot/reopen/CF、legacy 82 项 corpus、runtime/error/architecture/ArcMut 门禁证据与回滚点已冻结
  - [x] 独立兼容 ledger、根 package 29/32 与 37/45 顶层工作包盘点已冻结
- [x] PR-M06-10：实现 RocksDB MessageStore adapter 与 parity
  - [x] `rocketmq-store-rocksdb` canonical 拥有 derived backend、Timer/Transaction kernel 与 narrow Local WAL composition
  - [x] Store 旧 `RocksDBMessageStore` 路径仅保留 legacy DTO/trait/config 与 CommitLog dispatcher 投影
  - [x] 默认 Rocks 模式仅写 Rocks CQ/Index；`rocksdb_cq_double_write_enable=true` 才保留 Local 兼容镜像
  - [x] Local/Rocks pull parity、restart catch-up、offset-by-time、failure mapping 与无 uniq-key index 回归已覆盖
  - [x] 唯一 CommitLog、依赖方向、无 Client/Broker 泄漏、回滚点与独立兼容 ledger 已冻结
- [x] PR-M06-11：完成 Store facade、Tiered 反转与 feature 所有权
  - [x] `rocketmq-tieredstore` 只新增对 `rocketmq-store-api` 的中立生命周期依赖，无 Store/Local/Rocks 反向边
  - [x] Store `TieredStoreDecorator` 独占状态/结果映射，Local facade 只保 fallback、dispatch 与 lifecycle 组合
  - [x] Local 拥有 fast/safe/io_uring，Rocks 拥有 native rocks，Store 保留精确弱转发和 legacy alias
  - [x] no-default 继续编译 Local 兼容 facade，默认 feature 与 R0 public path 未关闭
  - [x] no-default/default/local/fast/safe/fast+safe/io_uring/rocks/tiered/observability 精确矩阵全部通过
  - [x] Tiered lifecycle、写入 dispatch、读取 fallback、fast+safe 优先级与 195 项 M06 contract 通过
  - [x] 独立兼容 ledger、7 条 ArcMut 一对一 relocation 与 39/43 顶层工作包盘点已冻结
- [x] PR-M06-12：完成依赖图与消费方收口
  - [x] root workspace 统一登记 API/Local/Rocks/Tiered dependency，当前 29/32，剩余仅三个 Proxy package
  - [x] architecture policy 新增 Store facade 禁止反向依赖 Client/Broker/NameServer/Controller/Proxy 的规则
  - [x] storage 子图精确冻结为 `api ← local ← rocks`、`api ← tiered` 与 `store → 四个 owner`
  - [x] Broker send processor 直连 `MessageAppender + StoreHealth`；Store facade consumer 固定为 Broker/Proxy/store-inspect
  - [x] 四个 standalone Cargo 项目无 storage 直接边，canonical/legacy compile 与十项 feature matrix 通过
  - [x] 完整 M06 contract 200/200、architecture/runtime/ArcMut/routing 与 consumer all-feature checks 通过
  - [x] closeout ledger、回滚点、M07/M09/M10 交接物与 40/42 顶层工作包盘点已冻结
- [x] 对应任务文档的 Exit Checklist 全部通过

### M07 Legacy Runtime 排空与 Client 依赖边收敛

任务文档：[`07-legacy-and-client-edge-burn-down.md`](phase-2-core-boundaries/07-legacy-and-client-edge-burn-down.md)

- [x] PR-M07-01：将 `rocketmq-rust` 生命周期能力迁入 runtime
  - [x] schedule/task/shutdown/signal canonical owner 已迁入 `rocketmq-runtime`，legacy 只保精确 re-export shim
  - [x] workspace 的 15 个 lifecycle consumer 文件已改用 runtime canonical path；Example 的 8 个 signal 旧路径作为 standalone 兼容面冻结
  - [x] 新边界 crate 禁止依赖 `rocketmq-rust` 的显式 architecture policy 与 6 项 source contract 已落地
  - [x] runtime 77 项、legacy 36 项、新旧路径差分 3 项、Broker scheduled 1 项、Client scheduled 4 项、Store 484 项通过
  - [x] Example、Tauri backend、Web backend 按最近 AGENTS 的 fmt/Clippy/build 累计路线通过；未修改 dashboard-common，未触发 GPUI
  - [x] Runtime audit、architecture guard/fixtures、ArcMut 63 项/24 fixtures、AGENTS routing 通过；typed-error 仅剩 main 既有 11 项
  - [x] ADR-013 批准 10 条既有 ArcMut import 一对一 relocation，台账保持 1,170 identities/3,232 occurrences
- [x] PR-M07-02：删除 MCP 冗余 Client 边
  - [x] MCP manifest/lockfile 不再直接依赖 Client/common/remoting；Client 仅经 `rocketmq-admin-core` 间接进入 normal closure
  - [x] QueryFacade/AdminSession 继续使用 admin-core，4 项 source/manifest/lockfile/policy contract 防止 Client 与 facade 绕行回流
  - [x] 默认 8 个只读/诊断 Tool 与 5 个 change-planning Tool 合同保持；5 类计划逐项验证 `mutates_cluster: false`
  - [x] MCP default 72 unit + 2 integration、all-features 89 unit + 2 integration、streamable HTTP Clippy 与 Rustdoc 通过
  - [x] Architecture 35 项/fixtures、ArcMut 63 项/24 fixtures、AGENTS routing、workspace fmt/Clippy 与 diff check 通过
  - [x] Error hygiene 仅复现 main 既有 11 项；本包未新增 finding，回滚不得恢复 MCP 直接 Client/common/remoting 边
- [x] PR-M07-03：完成 NameServer RouteLookup 反转
  - [x] `ClusterTestRouteLookup` 已冻结为注入式异步 port；默认 adapter 只以 canonical protocol 构造 route request，并经 transport 发出
  - [x] `productEnvName` top-addressing、端点缓存与 route fallback 兼容语义保留；解析、DNS 和所有端点尝试共用单个 3 秒绝对 deadline
  - [x] 默认 adapter 由 `ServiceContext` 子上下文拥有，shutdown 可等待；未创建第二套 runtime、MQClientManager 或 admin registry
  - [x] NameServer manifest、lockfile、源码与 architecture baseline 的 Client 直接边均已删除；root Client consumer 基线降至 3
  - [x] 成功/缓存、timeout、不可达、processor route fallback、活动解析 shutdown 与集成 remoting 行为已覆盖
  - [x] NameServer 179 unit、20 integration/bin、8 doc（1 ignored）、strict Clippy、runtime/architecture/ArcMut/routing guard 通过
  - [x] ArcMut 台账净减少 3 identities/6 occurrences 至 1,167/3,226；error hygiene 仅复现 main 既有 11 项
- [x] PR-M07-04：清零 Broker Client 边
  - [x] 远程 send/pull 结果归 model，本地 transaction/POP 读取归 store-api 拥有型 `ReadOutcome`
  - [x] Broker-owned publish route 与 out-api adapter 已落地，query assignment、escape bridge、lite/runtime fixture 已迁移
  - [x] Broker manifest/source/normal closure 的完整 Client 直接边均为 0，workspace manifest consumer 基线降至 2
  - [x] 聚焦行为测试、all-targets、all-feature/RocksDB Clippy、workspace fmt/Clippy 与 architecture/ArcMut/routing guard 通过
  - [x] 全量 Broker 测试的既有 25 项失败已如实记录，未计为通过；error guard 的既有 11 项 finding 同样未计为通过
  - [x] ArcMut 台账由 1,167/3,226 降至 1,163 identities/3,216 occurrences，零新增债务
- [x] PR-M07-05：收敛 Admin contract 与 Client adapter
  - [x] Topic/Broker/Consumer/Security/Lite capability、request、result 与 admin-owned `AdminError`/`Clock` 已进入纯 `core/`
  - [x] Client 实现与旧业务编排已集中到 `src/client_adapter/`；R0 `DefaultMQAdminExt` 路径和旧签名继续编译
  - [x] static-topic 纯 planner 使用单次 Clock 采样、确定性 broker 分配和 checked epoch；文件读写及 `.bak` 语义归 CLI
  - [x] no-default、client-adapter、legacy default 三套 feature 测试与严格 Clippy 通过，MCP 改为显式 client-adapter
  - [x] MCP default/all-feature test、streamable-http strict Clippy、Rustdoc，CLI 文件测试与 TUI check 通过
  - [x] Example、Tauri backend、Web backend 按最近 AGENTS 完成 fmt/Clippy；Web backend all-target/all-feature build 通过
  - [x] architecture baseline、119 项治理测试、runtime audit、ArcMut guard/24 fixtures 通过；ArcMut 降至 1,155/3,207
  - [x] 目标态差距由 153 降至 115，Admin Core 完全退出 Client source 与违规 DAG 清单
- [x] PR-M07-06：迁移 Web/Tauri Dashboard
  - [x] Tauri/Web backend 均显式使用 admin-core `default-features = false, features = ["client-adapter"]`，直接 Client/common/remoting 清单与源码边清零
  - [x] 管理查询、普通/事务测试发送、message/trace、Consumer/Topic/Broker/ACL 与 NameServer/VIP/TLS 配置变更统一经 Admin Session/facade，且不修改进程级环境变量
  - [x] Tauri message page cache 使用 dashboard-owned QueueKey 与 admin-owned QueueRef；无 model/protocol 绕界、完整 MQAdminExt 生命周期或自建 producer/runtime
  - [x] Tauri 76 tests、Web backend 23 tests、两项目 strict Clippy/build 与两套 Node production build 通过；Admin dashboard 6 项专项测试及 Admin/CLI/TUI 兼容测试通过
  - [x] architecture baseline、8 项 M07-06 contract、98 项治理测试、24 fixtures、runtime/ArcMut/routing guard 通过
  - [x] 目标态差距由 115 降至 87；父项关闭后 46/82 已完成、36 尚未完成，唯一下一工作包为 PR-M07-07
- [x] PR-M07-07：完成 allowlist 与 consumer closeout
  - [x] manifest allowlist 按 caller/target/kind/path/alias 精确匹配，source allowlist 按 caller/path/alias 精确匹配
  - [x] 永久 Admin/Example allowlist 与临时 baseline 分离；Client 临时账本只剩 Proxy 1 manifest + 13 source，owner/remove_by 为 Proxy/M08
  - [x] Proxy 22 处中立 Send/Pull DTO 改用 model canonical path，真正 Client runtime 只剩 cluster/remoting 两文件
  - [x] Broker、NameServer、proxy-core/local、common、remoting normal closure 固定禁止到达 Client，违规 fixture 覆盖绕行
  - [x] 根、Proxy、Admin/MCP、Example、Tauri、Web、frontend 与 governance 累计验证完成；GPUI 未被本次变更触发
  - [x] [`M08 交接清单`](phase-2-core-boundaries/07-client-edge-closeout-handoff.md) 已冻结 owner、转换 seam、临时账本和 lifecycle 风险
  - [x] 目标差距由 87 降至 66；47/82 已完成、35 未完成，下一工作包 PR-M08-01
- [x] 对应任务文档的 Exit Checklist 全部通过

### M08 Proxy Core、Cluster、Local 三向物理拆分

任务文档：[`08-proxy-three-way-split.md`](phase-2-core-boundaries/08-proxy-three-way-split.md)

- [x] PR-M08-01：创建 `rocketmq-proxy-core` 与 proto owner
  - [x] 根 workspace 达到 30/32；Core 成为 proto/error/status/context/session/identity/ingress config 唯一物理 owner
  - [x] `definition.proto`/`service.proto` SHA-256 golden、唯一 build/include owner和 generated client/wire contract 已冻结
  - [x] 旧 Proxy 的 proto/error/status/context/session/config/root 路径保持精确 re-export 或 Channel 专用 type alias
  - [x] Context 对认证证明泛型化并使用 transport connection metadata；白名单信任位保持 facade 私有；Session 使用泛型 Channel slot
  - [x] Core default/no-default、34 项 test、Proxy 113 项 unit/bin/compat/gRPC/remoting test 与两 crate strict Clippy 通过
  - [x] Core manifest/source/normal closure 无 Client、admin-core、Broker、store、auth provider、common、remoting 或 legacy facade
  - [x] 目标差距保持 66 且 Core 零 finding；48/82 已完成、34 未完成，下一工作包 PR-M08-02
- [x] PR-M08-02：迁移中立 plan、port、service 与 ingress
  - [x] send/pull/pop/ack/route/transaction 的 request/plan/result、`MessagingProcessor` 与默认 processor 迁入 Core
  - [x] 六组 service port、`ServiceManager`、default/static service 与 `ResourceIdentity` 由 Core 唯一拥有
  - [x] Core-owned `ProxyMessage`/`ProxyMessageExt` 隔离 Common 消息类型；Metadata port 改用 Protocol `UserInfo`/`AclInfo`
  - [x] gRPC adapter/middleware/server lifecycle 与中立 admission/session/consumer/transaction/telemetry policy 迁入 Core
  - [x] Remoting request classifier、dispatch contract 与 status conversion 迁入 Core；cluster address resolution 留在 adapter
  - [x] gRPC transaction producer group 改由 Core Transaction port 提供，ingress 不再直接调用 Cluster backend
  - [x] Core 45 项 unit + 2 项 proto contract、Proxy 104 项 unit/bin/compat/gRPC/remoting test 全绿
  - [x] Core/Proxy default/no-default、根 30-package strict Clippy、runtime audit 与 Example/Tauri/Web standalone 累计路线通过
  - [x] target gap 保持 66 且 Core 零 finding；typed-error 仅剩 main 既有 11 项，未新增 Core/Proxy finding
  - [x] 49/82 已完成、33 未完成，下一工作包 PR-M08-03
- [x] PR-M08-03：创建 Cluster adapter
  - [x] `rocketmq-proxy-cluster` 已加入根 workspace（31/32），唯一拥有 Client instance/manager、Cluster service/manager、worker/cache/state 与 producer/consumer/route runtime
  - [x] Client callback、SendResult/PullResult 与 Message/MessageExt 在 Cluster 边界转换为 model/Core DTO，Client 类型未泄漏到 Core port
  - [x] Cluster 仅消费注入的 security-api `OutboundSigner`；auth provider composition 与敏感字段脱敏仍由 Proxy facade 负责
  - [x] Client worker、producer 与 instance 的启动、取消、shutdown/join 由 Cluster 持有；每个 adapter 使用独立 `ServiceContext` 子域，取消活动/排队工作后在一个绝对 deadline 内先停 producer、再停 Client
  - [x] Client 的 `ClientInstanceHandle` 隐藏原始共享可变载荷，Cluster 源码无 `ArcMut`；账本由 3207 降至 3191 个 occurrence
  - [x] Remoting lock/unlock 与 Cluster address resolution 迁入 Cluster；Proxy 保留兼容 wrapper，Core 只保留中立 classifier/dispatch/status contract
  - [x] 旧 cluster/config/service/root public path 保持精确 re-export，ProxyConfig Serde/default 与 canonical/legacy compile contract 保持兼容
  - [x] target guard 为 51（目标 DAG 直接边 49 + 传递闭包边 2）；Client 临时账本 manifest/source 均为 0，Cluster 直边/源码无 Broker/store/local/auth provider，backend closure 无 Broker/store/local
  - [x] Cluster 19、Proxy 101、Core 47、Client 聚焦 9 项与 Auth signer 1 项测试通过；architecture contract 120（含 M08 9）、ArcMut guard 65 + fixture 24 与 runtime audit 全绿；typed-error 仅复现 main 已登记的 11 项，零新增
  - [x] 50/82 已完成、32 未完成，下一工作包 PR-M08-04
- [x] PR-M08-04：创建 Local adapter
  - [x] `rocketmq-proxy-local` 已加入根 workspace（32/32），唯一拥有 Local Broker facade client、LocalServiceManager、message/consumer/route/transaction adapter 与 local lifecycle
  - [x] Proxy 旧 `local`/`config`/`service` public path 保持精确 re-export；Proxy manifest 不再直接依赖 Broker/Store，Local manifest 只使用允许的 Broker/Core/Model/Runtime/Error 边
  - [x] Local 通过 Broker 私有兼容 surface 消费协议实现类型，源码无 Common/Remoting/Store/Client/Cluster 直接 import，normal closure 无完整 Client 或 Cluster
  - [x] Local worker 由注入的 `ServiceContext` 子域持有，使用 1024 容量有界队列、取消令牌和单一 `ShutdownDeadline`；未注入 context 的历史构造 fail closed 为 typed startup error
  - [x] Local 8 项覆盖 send/pull/pop/ack/route/transaction、bounded queue 与 embedded lifecycle；Proxy 99 项兼容/ingress 测试通过，no-default、tieredstore 与 Local all-target/all-feature strict Clippy 通过
  - [x] baseline guard 通过；target guard 由 51 降至 49（目标 DAG 直接边 47 + 传递闭包边 2），无缺失计划 package，Core/Cluster/Local 均为零 finding
  - [x] architecture contract 354、ArcMut 实际 guard + fixture 24、runtime enforcing audit、32-package workspace fmt/strict Clippy 与 AGENTS routing 全绿；typed-error 仅复现 main 既有 11 项，零新增
  - [x] 51/82 已完成、31 未完成，下一工作包 PR-M08-05
- [x] PR-M08-05：将现有 Proxy 降为 composition/facade
  - [x] 删除 Proxy 未使用的 `rocketmq-rust` manifest/lockfile 直边，target gap 由 49 降至 48
  - [x] facade 继续以非 optional 方式依赖 Core/Cluster/Local，保持 R0 `default = []`；未提前定义下一 major mode feature
  - [x] ProxyConfig 继续持有 Serde/env/CLI envelope，并将 Core/Cluster/Local normalized config 交给各自 owner
  - [x] processor/service/cluster/local 等业务 owner 路径保持精确 re-export；Proxy 只保留 bootstrap/config/auth/observability/binary、gRPC/Remoting ingress adapter 与兼容导出
  - [x] 新增静态合同，禁止 Client/Broker/Store/legacy runtime 回流，并验证 Core/Local 不经 facade 反向到 Cluster/Client
  - [x] Proxy default/no-default 各 82 unit + 1 binary + 4 compatibility + 9 gRPC + 3 Remoting 共 99 项通过
  - [x] architecture contract 355、根 fmt/32-package strict Clippy、baseline guard、ArcMut、runtime audit、AGENTS routing 与 diff check 全绿；target 按预期精确剩余 48，typed-error 仅复现 main 既有 11 项
  - [x] 52/82 已完成、30 未完成，下一工作包 PR-M08-06
- [x] PR-M08-06：验证 feature closure 与下一 major fixture
  - [x] 新增 [`Proxy feature closure 证据`](phase-2-core-boundaries/08-proxy-feature-closure-evidence.md) 与机器可读下一 major fixture
  - [x] Core 47、Cluster 19、Local default/tiered 各 8、Facade 99 项行为/兼容/ingress 测试通过
  - [x] Core/Cluster/Local no-default、Facade no-default/observability/tiered feature check 全绿
  - [x] 7 项 closure contract 验证 R0 default=no-default、Local+Tiered、Facade observability 及 test/dev edge 分离
  - [x] Client allowlist 精确为 workspace 2 + standalone 1，Proxy 临时 manifest/source 例外均为 0
  - [x] 下一 major `cluster-mode`、`local-mode`、`compat-all-modes` 与 optional adapter 预期已冻结，但未进入 R0 manifest
  - [x] architecture contract 362、根 fmt/32-package strict Clippy、baseline、ArcMut、runtime、routing 与 diff check 全绿；target 精确剩余 48，typed-error 仅复现 main 既有 11 项
  - [x] R0 功能等价、下一 major 公告边界与 M08 Gate 已按批准的总体目标签署
  - [x] 53/82 已完成、29 未完成，下一工作包 PR-M09-01
- [x] 对应任务文档的 Exit Checklist 全部通过

### M09 Facade 收口与 32-Package Gate

任务文档：[`09-facade-and-package-closeout.md`](phase-2-core-boundaries/09-facade-and-package-closeout.md)

- [x] PR-M09-01：收口 workspace 与目标 DAG
  - [x] 根 workspace 精确固定为 32 package，standalone 项目未误纳入；目标模式已删除缺失 package 绕过参数
  - [x] TieredStore 的 `BoundaryType` canonical owner 下沉至 Model，Protocol/Common 只 re-export；Common 直接边与两条传递例外清零
  - [x] 严格 target 未授权 finding 从 48 降至 0；49 条活动 R0 兼容/组合边与 3 条 dev-only 边分别精确治理
  - [x] 临时 manifest/source 例外均为 0；兼容台账按 M09-02 11、M09-04 3、R1 29、next-major 4、long-term 2 冻结
  - [x] caller/target/kind/path/alias 改名、改 kind、重复增长和 dev→normal 提升反例均 fail closed
  - [x] Model 33、Protocol 1,373、TieredStore 56 与 architecture contract 370 项通过；根 fmt/strict Clippy、baseline/target/fixture、ArcMut、runtime、routing 与 diff check 全绿
  - [x] [`M09-01 收口证据`](phase-2-core-boundaries/09-target-dag-closeout-evidence.md) 已记录 guard 口径、回滚点和后续债务
  - [x] 54/82 已完成、28 未完成，下一工作包 PR-M09-02
- [x] PR-M09-02：完成 facade 与 legacy purity 审查
  - [x] 审计 Common、Remoting、Store、Proxy、`rocketmq-rust` 的允许 public path、canonical owner、composition 与退出窗口
  - [x] Filter 直连 Protocol owner 并移除 Common；Remoting 移除未使用 Macros；store-inspect 经 Store 精确 re-export 使用 Local parser 并移除 Common
  - [x] NameServer/Controller、Proxy Error/Model、Remoting Error/Runtime、Store Error/Runtime/Observability 8 条必要组合边纳入目标 DAG
  - [x] M09-02 到期 ledger 11 → 0；总 ledger 49 → 38，未延期、未增加临时 manifest/source 例外
  - [x] Store inspection facade 零算法/零 owner；IPv4 message ID、物理偏移与 `UNIQ_KEY` legacy 输出有运行 golden
  - [x] M09 contract 10、常规 architecture 215、相关 Store Local 4、facade/consumer 44、Filter 103、Store Inspect 3、Remoting 116、Proxy 82、legacy runtime 33 项通过
  - [x] [`M09-02 purity 证据`](phase-2-core-boundaries/09-facade-purity-closeout-evidence.md) 已记录职责表、11 条处置、行为差分与回滚边界
  - [x] 55/82 已完成、27 未完成，下一工作包 PR-M09-03
- [x] PR-M09-03：证明 public API、feature、wire/storage 兼容
  - [x] Rustdoc JSON 基线覆盖根 workspace 全部 31 个 library/proc-macro target；默认 feature public API diff 为 0
  - [x] Protocol/Transport/Store/Admin/Proxy 的 24 条 feature/default 命令通过；Store 十项矩阵逐项执行
  - [x] wire/canonical-legacy 6/6、20-byte CQ/Index/CommitLog/Rocks storage 10/10 通过；完整矩阵 40/40
  - [x] R0 默认值保持不变；Proxy 下一 major 的 cluster/local/compat mode features 均未提前启用
  - [x] additive 0、deprecated 0、breaking 0、unclassified 0，无需批准例外或修复 breaking
  - [x] [`M09-03 兼容证明`](phase-2-core-boundaries/09-public-api-feature-wire-storage-evidence.md) 已记录工具链、命令、结果、超时处置和回滚边界
  - [x] 56/82 已完成、26 未完成，下一工作包 PR-M09-04
- [x] PR-M09-04：验证 Client allowlist 与跨项目消费者
  - [x] Client manifest/source allowlist 精确为 workspace 2（Admin adapter、Proxy Cluster）+ standalone 1（Example）
  - [x] MCP 未使用 Auth/Error direct edges 物理删除；Runtime owned lifecycle 边纳入目标 DAG；M09-04 ledger 3 → 0
  - [x] 总 compatibility/composition ledger 38 → 35；剩余 R1 29、next-major 4、long-term 2
  - [x] MCP/Tauri/Web backend 均只经 Admin Core client-adapter 到达 Client，无 Client/Common/Remoting 绕行
  - [x] MCP default 72+2、all-feature 89+2、HTTP strict Clippy/Rustdoc 通过；外部集群 E2E 1 项按环境 ignored
  - [x] Example、Tauri frontend/backend、Web frontend/backend 全部按最近 AGENTS 验证通过；GPUI 条件未触发
  - [x] [`M09-04 跨项目证据`](phase-2-core-boundaries/09-client-allowlist-cross-project-evidence.md) 已记录 allowlist、三条处置、锁文件差分、提示与回滚边界
  - [x] 57/82 已完成、25 未完成，下一工作包 PR-M09-05
- [x] PR-M09-05：准备 R0/R1/下一 major 发布包
  - [x] 32-package publish order 按目标 DAG 固定，并保留六阶段 conceptual release chain
  - [x] R0 release notes 完整列出 10 个新 crate、canonical/deprecated owner、无行为变化声明与回滚
  - [x] R1 consumer plan 精确覆盖 12 个 caller、29 条兼容边；CI baseline/release guards 禁止新增或扩张
  - [x] 外部用量采集覆盖 crates.io、GitHub code search、Issue/Discussion 与 release feedback，未知信号 fail closed
  - [x] next-major 精确列出 4 条依赖边及 admin legacy、common compat、remoting 深路径、Proxy mode feature 范围
  - [x] 2 条长期 Store composition 边明确排除；破坏性删除仍须 next-major 独立证据 Gate
  - [x] [`M09-05 发布包证据`](phase-2-core-boundaries/09-r0-r1-next-major-release-package-evidence.md) 已记录机器合同、CI、验证与回滚边界
  - [x] 58/82 已完成、24 未完成，下一工作包 PR-M09-06
- [x] PR-M09-06：冻结快照并执行 Phase 2 Gate
  - [x] 候选实现提交 `490c583e94b31dc7ae1b83c55ed811e2b90d4cce` 与 tree `e959367d3b4002653e4e25e5b0c19213de8766b5` 已冻结
  - [x] 初始 typed-error 11 项阻塞全部修复；最终错误架构 14/14 类通过
  - [x] public API 31/31 零差异；feature 24/24、wire 6/6、storage 10/10，总矩阵 40/40
  - [x] MCP、RocksDB、根 workspace 与 Example/Tauri/Web routed consumer 门禁通过；GPUI 条件未触发
  - [x] [`M09-06 Phase 2 Gate 证据`](phase-2-core-boundaries/09-phase-2-gate-evidence.md) 已绑定 DEV/REV/TEST/ARCH/HUMAN 结论
  - [x] 59/82 已完成、23 未完成，下一工作包 PR-M10-01
- [x] 对应任务文档的 Exit Checklist 全部通过

### Phase 2 Gate

- [x] 根 workspace 精确包含 32 个 package
- [x] 10 个新 crate 的禁止依赖边为零，目标 DAG 无环
- [x] 完整 Client 直接消费者收敛为 workspace 2 个、standalone 1 个
- [x] `proxy-core`/`proxy-local` 的传递闭包不含完整 Client
- [x] canonical/legacy API、wire、storage、Serde 与 feature 兼容 fixture 全部通过
- [x] facade/legacy ledger 只下降，所有剩余项有 owner 和退出里程碑
- [x] `[ARCH]`、`[REV]`、`[TEST]` 已签署
- [x] `[HUMAN]` 已批准进入 Phase 3

## 5. Phase 3：生产就绪

### M10 耐久派生引擎与可量化性能

任务文档：[`10-durability-and-performance.md`](phase-3-production-readiness/10-durability-and-performance.md)

- [x] PR-M10-01：建立派生 cursor 合同和 replay harness
  - [x] per-engine cursor 只连续推进，使用 `(source_epoch, physical_offset, length)` 幂等键
  - [x] version 1 checkpoint 固定 32 bytes、带 CRC32 且不含 payload/第二 WAL
  - [x] owner 仅在 durable persistence 成功后发布 cursor；不改变 AppendReceipt/主写 ack
  - [x] Store API 7/7 与 replay 7/7 覆盖崩溃、重复、脏尾、损坏、升级和 engine 隔离
  - [x] public API additive diff 已审核并最终 31/31 零差异；ArcMut/依赖/runtime/error guard 通过
  - [x] [`M10-01 证据`](phase-3-production-readiness/10-derived-cursor-replay-evidence.md) 记录基线失败、验证与回滚
  - [x] 60/82 已完成、22 未完成，下一工作包 PR-M10-02
- [x] PR-M10-02：实现 Tiered cursor、retry ledger 与背压
  - [x] 顺序读取 CommitLog；channel count/bytes 满时把背压传回 reput，不丢事件
  - [x] 失败记录与 cursor 原子持久化；ledger 仅保存 offset tuple 与 retry metadata，不复制 payload
  - [x] count/bytes/age 三类硬上限触发 `readiness=false`，并 pin 最小未解决 WAL segment
  - [x] provider timeout/partial write/restart/duplicate/ledger full/WAL pin corpus 7/7 通过
  - [x] retry scheduler 由 `ScheduledTaskGroup` 所有；正常 shutdown 排空，取消释放阻塞 sender
  - [x] Store 82+9、Broker Rocks 20、POP 4 专项通过；Rocks batch flush 回归已由原测试捕获并修复
  - [x] public API additive diff 已审核并最终 31/31 零差异；ArcMut/依赖/runtime/error/MCP 门禁通过
  - [x] [`M10-02 证据`](phase-3-production-readiness/10-tiered-cursor-retry-evidence.md) 记录目标、基线失败、验证与回滚
  - [x] 61/82 已完成、21 未完成，下一工作包 PR-M10-03
- [x] PR-M10-03：优化 CQ、Rocks 与 Tiered 读取
  - [x] Local CQ 直接借用 mmap slice 解码 20B unit，按有界请求预分配结果，不为单元创建临时 buffer
  - [x] Rocks CQ 使用一次原生 range scan 返回 typed value，移除 typed→Bytes→typed 往返
  - [x] Tiered 使用全局 byte-bounded、generation-aware block cache，并合并相邻 CQ/CommitLog range
  - [x] 32 条冷拉精确 2 次 provider read、热拉 0；Rocks 完整拉取精确 2 次 point read + 1 次 scan
  - [x] cache retained bytes、generation/path 失效与 body lease 生命周期已审查并由测试覆盖
  - [x] public API additive diff 已审核；ArcMut/依赖/runtime/error/MCP/Rocks 专项门禁通过
  - [x] [`M10-03 证据`](phase-3-production-readiness/10-read-path-optimization-evidence.md) 记录目标、基线失败、验证与回滚
  - [x] 62/82 已完成、20 未完成，下一工作包 PR-M10-04
- [x] PR-M10-04：实现 Index/Compaction generation
  - [x] Index/Compaction 使用 versioned `gen-N.tmp`、CRC/条数/边界校验、数据/目录 sync 与原子 CURRENT
  - [x] reader lease 延迟删除 retired generation；重启清理 tmp/orphan，损坏 current 只回滚到 validated previous
  - [x] Compaction generation 复制 live record，内存仅保 offset/代内 payload 位置，不依赖旧 CommitLog payload
  - [x] durable compaction watermark 参与 replay；Recovering/OffsetFoundNull fail closed，不裸扫描 CommitLog
  - [x] Index 与 Compaction build/sync/rename/CURRENT/cleanup kill/restart corpus 全部通过
  - [x] Store 486 library、Tiered 66+1+7、Rocks 82+9、Broker 20+4、MCP 与 workspace strict Clippy 通过
  - [x] public API 31/31 零差异；依赖/ArcMut/runtime/error/release/routing 门禁通过
  - [x] [`M10-04 证据`](phase-3-production-readiness/10-index-compaction-generation-evidence.md) 记录目标、基线/环境失败、验证与回滚
  - [x] 63/82 已完成、19 未完成，下一工作包 PR-M10-05
- [x] PR-M10-05：建立 benchmark、soak 与性能 Gate
  - [x] 固定 8 个 profile、11 个变体和 50 个 profile 指标合同；现有 Criterion 命令只作为局部参考
  - [x] guard 强制完整环境、正确性证据、原始 sidecar hash、至少 5 次样本和 baseline/candidate 环境一致
  - [x] 吞吐、p99、RSS、allocation 与 I/O amplification 使用方向敏感的 5% 硬门禁
  - [x] MAD/样本偏离噪声 fail closed；提升目标与 provider/native call 仅作为非门禁 hypothesis
  - [x] 例外要求 owner、批准人、期限和回退配置，且不能覆盖 correctness/schema/环境/噪声失败
  - [x] guard 聚焦测试 11/11、全架构 guard 125/125 与 dependency/release/ArcMut/routing 门禁通过
  - [x] [`M10-05 证据`](phase-3-production-readiness/10-performance-gate-evidence.md) 明确区分 fixture 与真实测量
  - [x] 64/82 已完成、18 未完成，下一工作包 PR-M11-01
  - [ ] 真实固定硬件 baseline/candidate、原始数据 hash 与 `[HUMAN]` M10 Gate 待签署
- [ ] 对应任务文档的 Exit Checklist 全部通过

### M11 安全、可观测性与云原生生产化

任务文档：[`11-security-observability-cloud.md`](phase-3-production-readiness/11-security-observability-cloud.md)

- [x] PR-M11-01：建立 Telemetry semantic registry
  - [x] 119 个 metric、4 个 span、7 个 stable log event 与 66 个 attribute 进入 versioned registry
  - [x] guard 与 Rust semantic/catalog/span/event/outage 常量双向同步，拒绝未知信号、隐私/基数/采样/deprecation 漂移
  - [x] 7 类故意违规 fixture 与 7 个 guard 单测通过；全架构 guard 132/132 通过并接入 Linux/Windows CI
  - [x] collector outage queue 同时限制 count/bytes/record，`try_enqueue` 不等待并计量 drop；provider 共享绝对关闭预算
  - [x] observability 精确 7 组 feature matrix 的 workspace check、strict Clippy 和 package test 全部通过
  - [x] [`M11-01 证据`](phase-3-production-readiness/11-telemetry-semantic-registry-evidence.md) 记录 API、验证、失败修复与回滚边界
  - [x] 65/82 已完成、17 未完成，下一工作包 PR-M11-02；M10/M11/Phase 3 Gate 均未提前宣称完成
- [x] PR-M11-02：实现 SecretProvider 基础合同与本地 adapter
  - [x] `rocketmq-security-api` 冻结同步、运行时中立的 provider/name/version/capability/error 合同
  - [x] `SecretMaterial` 禁止空值、Debug 恒定 redaction，并在显式 zeroize 与 Drop 时清零底层字节
  - [x] `rocketmq-auth` 提供无全局单例的显式 registry；缺失与重复 provider 均 fail closed
  - [x] 环境 adapter 只读取显式 logical-name→env allowlist，保持只读且不输出变量名/值
  - [x] 本地文件 adapter 使用 AES-256-GCM、name+version AAD、owner-only 权限、不可覆盖版本和原子发布
  - [x] Windows 在没有 owner-only ACL verifier 前拒绝启用；WSL/Linux 真实权限、加密、tamper、版本冲突测试通过
  - [x] [`M11-02 证据`](phase-3-production-readiness/11-secret-provider-evidence.md) 记录合同、平台测试、API 增量和回滚边界
  - [x] 66/82 已完成、16 未完成，下一工作包 PR-M11-03；安全默认值、M10/M11/Phase 3 Gate 均未提前宣称完成
- [x] PR-M11-03：实现 Secure Profile 与一次性 bootstrap
  - [x] 新部署未指定 profile 时解析为 `secure`；已识别的既有部署保持 `compatibility` 并强制输出持久化迁移状态
  - [x] unknown profile、缺 trust anchor/provider/身份引导、多个身份源、缺失或过期 bootstrap、非 TLS listener 和降级请求均 fail closed
  - [x] 一次性 grant 使用至少 32B proof，绑定 cluster/listener/expiry 并以 constant-time digest 校验；原始 proof 不持久化
  - [x] owner-only 状态按 available→claimed→consumed 原子推进；并发、重启、重放、损坏状态及 provisioner 失败均不重开
  - [x] Windows 在没有 owner-only ACL verifier 前拒绝启用；WSL/Linux 真实原子文件、权限和并发测试 7/7 通过
  - [x] [`M11-03 证据`](phase-3-production-readiness/11-secure-profile-bootstrap-evidence.md) 记录 profile、状态机、公共 API 和剩余边界
  - [x] 67/82 已完成、15 未完成，下一工作包 PR-M11-04；M10/M11/Phase 3 Gate 均未提前宣称完成
- [x] PR-M11-04：实现 credential/certificate rotation 与原子 reload
  - [x] `CredentialRotationManager` 以不可变 ArcSwap 快照发布 active/retiring/revoked/break-glass 状态，写者串行且读者只观察完整代际
  - [x] SecretProvider 每次只读取一个 versioned bundle；provider、parser、version mismatch、partial/invalid 候选均保留当前 generation
  - [x] overlap 到期后撤销旧材料；rollback 只恢复未撤销且未过期的 last-known-good，并撤销失败候选
  - [x] break-glass 默认禁用、限时且必须带 typed reason；rotation/revoke/rollback/启停均 audit-first，audit sink 失败不发布
  - [x] TLS certificate/key/trust 全量构建后原子发布；手动 reload 与 watcher 写者串行，generation 单调，失败保持 last-known-good
  - [x] Windows 与 WSL/Linux 覆盖 6 项 credential contract、真实证书不匹配/无效 PEM/LKG，以及 8 路并发 TLS reload
  - [x] [`M11-04 证据`](phase-3-production-readiness/11-credential-rotation-evidence.md) 记录状态机、API 增量、验证矩阵与回滚边界
  - [x] 68/82 已完成、14 未完成，下一工作包 PR-M11-05；M10/M11/Phase 3/HUMAN Gate 均未提前宣称完成
- [x] PR-M11-05：完成 MCP HTTPS、JWKS 与 Principal 传播
  - [x] Streamable HTTP listener 改为 TLS enforcing，复用 M11-04 原子 certificate generation/reload/LKG；真实 HTTPS 成功且同端口明文失败
  - [x] 公共 resource metadata 与 `WWW-Authenticate resource_metadata` 使用绝对 HTTPS URI，Host/Origin/body/timeout 边界保持生效
  - [x] OAuth 仅接受带 `kid` 的 RS256；HTTPS-only/无重定向/有界 JWKS fetch，完整校验后原子发布不可变 generation
  - [x] TTL/unknown-kid refresh、duplicate/symmetric/alg/use/key_ops 拒绝、fetch/parse 失败保持 bounded last-known-good 均有测试
  - [x] verified principal/client/roles/scopes/allowed_clusters 经真实 MCP `tools/call` 进入 RBAC、rate-limit 与 audit，HTTP 上下文缺失不回退 `local-stdio`
  - [x] 默认 stdio 与 `--all-features` 均通过；`change-planning` 仍只产生 `mutates_cluster: false` 计划，没有 Apply/`dangerous-tools`
  - [x] [`M11-05 证据`](phase-3-production-readiness/11-mcp-https-jwks-evidence.md) 记录 TLS/JWKS/principal、公共 API、验证矩阵和回滚边界
  - [x] 69/82 已完成、13 未完成，下一工作包 PR-M11-06；M10/M11/Phase 3/HUMAN Gate 均未提前宣称完成
- [x] PR-M11-06：完成 MCP Audit Writer 与 Shutdown Drain
  - [x] audit schema 固定为 `schema_version = 1`，principal/action/outcome/error 等变长字段先脱敏、清理控制字符并按 UTF-8 字节确定性截断
  - [x] 生产端只使用 `try_acquire_many_owned` + `try_send`；最大单条、队列条数与队列字节分别有界，overflow/oversized/closed 均独立计量
  - [x] writer 在 `ServiceContext` 下 FIFO 写入；文件创建、append 与 `sync_all` 全部经注入的 `BlockingExecutor`，sink 失败不输出底层路径或 I/O 错误
  - [x] `shutdown_with_deadline` 按“关闭准入→drain→flush→runtime shutdown”执行，所有阶段复用同一个绝对 `ShutdownDeadline` 并返回 accepted/written/dropped/pending/failure 报告
  - [x] count/byte overflow、oversized/redaction、FIFO、sink/flush failure、stall/deadline、运行时取消和真实文件 flush 共 7 项 focused test 通过
  - [x] 默认 82 tests 与 all-features 104 tests 通过；stdio/HTTPS/JWKS/principal 与无副作用 `change-planning` 合同保持不变
  - [x] [`M11-06 证据`](phase-3-production-readiness/11-mcp-audit-drain-evidence.md) 记录实现、API 增量、验证矩阵、回滚和未签署 Gate
  - [x] 70/82 已完成、12 未完成，下一工作包 PR-M11-07；M10/M11/Phase 3/HUMAN/ARCH Gate 均未提前宣称完成
- [x] PR-M11-07：建立容器镜像基础
  - [x] 新增独立 `Dockerfile.base`，builder/runtime manifest 使用 reviewed digest，Rust nightly 与 Debian package snapshot 固定日期；旧组合镜像行为不变并登记为 M11-08 到期例外
  - [x] runtime foundation 只从 pinned Debian runtime stage 构建，固定 UID/GID 10001、read-only rootfs、data volume/tmpfs、SIGTERM 和 OCI label 合同，无 shell/service entrypoint
  - [x] `container-policy.json` 冻结五服务 GHCR 命名、immutable tag、工具版本、零 CRITICAL、Sigstore bundle 与 digest-only keyless image signature 规则
  - [x] 静态 guard 与 6 组正向/故意违规测试通过，覆盖 mutable base、root、未固定 action、弱化 scanner/signature、未登记 Dockerfile、过期例外和 snapshot package 漂移
  - [x] workflow action 全部按 40 位 SHA 固定；PowerShell AST、Actionlint v1.7.12、Hadolint v2.14.0 与 AGENTS routing 检查通过
  - [x] Ubuntu workflow 已交付 build、non-root/read-only smoke、CycloneDX SBOM、Trivy、Cosign bundle 与 provenance artifact；本机/WSL 缺少容器和供应链工具，未把远端 workflow 写成已执行
  - [x] [`M11-07 证据`](phase-3-production-readiness/11-container-foundation-evidence.md) 记录 immutable 输入、策略、验证边界、兼容例外与回滚
  - [x] 71/82 已完成、11 未完成，下一工作包 PR-M11-08；M10/M11/Phase 3/HUMAN/ARCH 及容器动态 `[TEST]` Gate 均未提前宣称完成
- [x] PR-M11-08：交付五个服务镜像入口
  - [x] 删除到期的组合 `docker/Dockerfile` 与兼容例外；`Dockerfile.base` 只通过五个显式 target 生成 Broker/NameServer/Controller/Proxy/MCP 镜像，每个 runtime 仅含 owner binary
  - [x] 五服务均使用直接 JSON entrypoint、必需 config mount、`/var/lib/rocketmq/<service>` 数据路径、显式端口、UID/GID 10001、read-only rootfs/tmpfs 与 SIGTERM 标签合同，不含 secret 命令行参数
  - [x] Controller、Proxy、MCP stdio/HTTPS 改用 `rocketmq-runtime::wait_for_signal_result`；Broker/NameServer 已有的跨平台 SIGINT/SIGTERM 行为保持一致
  - [x] policy/guard 与 9 组正向/故意违规测试通过，覆盖五 target/owner/entrypoint/config/data/port/signal、遗留镜像复活、shell dispatch、secret 参数和弱化动态 smoke
  - [x] 五份 smoke 配置由真实服务二进制解析；workflow 已交付逐镜像 build、配置缺失 fail-closed、只读/volume、真实 SIGTERM、CycloneDX、Trivy 与 Cosign/provenance
  - [ ] Docker/Syft/Trivy/Cosign 本机不可用；M11-07 远端 build 已暴露 slim runtime 缺 CA bundle，M11-08 增加“签名 snapshot HTTP 引导 CA→HTTPS”修复，但未观察修复后五镜像动态套件成功，容器 `[TEST]` Gate 保持开放
  - [x] [`M11-08 证据`](phase-3-production-readiness/11-service-image-entrypoints-evidence.md) 记录 owner 合同、signal 接线、验证边界和逐服务 digest 回滚
  - [x] 72/82 已完成、10 未完成，下一工作包 PR-M11-09；M10/M11/Phase 3/HUMAN/ARCH 及容器动态 `[TEST]` Gate 均未提前宣称完成
- [x] PR-M11-09：交付 Helm 与 Kustomize 资产
  - [ ] 入口 `[ARCH]`：五服务 schema/资源/state/secret 边界已版本化；production 签名 digest 与目标集群三个 Controller Service IP 尚未冻结，测试 fixture 不签署入口 Gate
  - [x] canonical Helm chart 与确定性 Kustomize base/secure overlay 交付 37 个资源；默认 digest/IP sentinel fail closed，base 镜像归零
  - [x] Broker/NameServer/Controller 使用 StatefulSet 与 Retain PVC；Proxy 保持 stateless；MCP 单副本 Recreate 且独占 retained audit PVC
  - [x] requests/limits、PDB、hostname/zone topology、default-deny NetworkPolicy、restricted Pod Security、UID/GID 10001 与外部 Secret/SecretProviderClass 引用合同闭合
  - [x] Controller remoting/Raft 修正为 60109/60110；三份 ordinal config 使用稳定 ClusterIP，显式多成员 bootstrap 仅由最小 node ID 执行；真实 formed quorum 留给 M11-11
  - [x] Helm v4.2.3、Kustomize v5.8.1、Kubeconform v0.8.0 archive hash 固定；lint/template/build、37/37 双 render schema、deterministic parity 与 8 组正负测试通过
  - [x] M11-09 禁止 probe/preStop/lifecycle/grace，避免提前伪造 M11-10 readiness/drain 语义；五份 rendered 配置经真实二进制解析
  - [x] [`M11-09 证据`](phase-3-production-readiness/11-helm-kustomize-assets-evidence.md) 记录部署边界、Controller/MCP 修正、工具链、未签署 Gate 与不降级 PVC 的回滚策略
  - [x] 73/82 已完成、9 未完成，下一工作包 PR-M11-10；M10/M11/Phase 3/HUMAN/ARCH、容器动态 `[TEST]` 与集群 fault Gate 均未提前宣称完成
- [x] PR-M11-10：统一 Probe、PreStop 与 Drain
  - [x] 共享 `rocketmq-runtime::ServiceLifecycle` 固定 `Starting/Ready/Draining/Stopped/Failed`；Broker、NameServer、Controller、Proxy、MCP 只在真实依赖和监听器就绪后发布 Ready
  - [x] 独立 HTTP health boundary 提供 `/readyz`、`/livez`、`/drainz`；TCP/startup probe 被 guard 禁止，health 8088 不通过 Kubernetes Service 暴露
  - [x] 第一次 preStop/SIGINT/SIGTERM/internal 请求冻结 45 秒绝对 deadline；RuntimeOwner、服务 drain、后台任务与 telemetry 只消费剩余预算，重复请求不延长
  - [x] Helm/Kustomize 五工作负载统一 60 秒 grace、HTTP readiness/liveness/preStop 与 lifecycle env；双 render 37/37 schema 和 12 组 Kubernetes 正负测试通过
  - [x] runtime 状态机、Proxy 双监听 barrier、Broker deadline/final flush、NameServer drain、Controller shutdown、observability budget 与 MCP 83-test/HTTP Clippy/Rustdoc 门禁通过
  - [x] [`M11-10 证据`](phase-3-production-readiness/11-probe-prestop-drain-evidence.md) 记录状态语义、阶段顺序、验证矩阵、未签署动态 Gate 与不恢复假 readiness 的回滚边界
  - [x] 74/82 已完成、8 未完成，下一工作包 PR-M11-11；M10/M11/Phase 3/HUMAN、容器动态 `[TEST]` 与 Kind/K3d fault/已确认消息恢复 Gate 均未提前宣称完成
- [x] PR-M11-11：完成 Kind/K3d fault matrix 实现与证据门禁
  - [x] Kind v0.27.0/Kubernetes 1.32.2、K3d v5.9.0、1 control-plane + 3 workers、Controller 3/2 quorum 和 storage class profile 已版本化
  - [x] `kind-architecture-refactor-e2e.ps1` 实现 rolling upgrade、node eviction、collector outage、disk-pressure taint、Controller leader failure、secret rotation 与 acknowledged recovery
  - [x] 断言覆盖 message ID、Queue/CommitLog offset、PVC UID、quorum、preStop、SLO、五镜像回滚和 fault cleanup，不以 Pod Ready 单独判定成功
  - [x] production evidence 强制 `dynamic_execution=true`/`fixture=false`，并校验 policy/chart/overlay/image/artifact SHA-256；fixture 必须显式 opt-in
  - [x] test-only fault-driver、管理 CLI 环境 ACL HMAC-SHA256、manual dynamic workflow 与 11 组正负证据测试落地
  - [ ] 本机无 Docker/Kind/K3d/Kubectl/Helm、目标签名镜像和 Secret，七场景真实动态执行未运行，Kind/K3d Gate 保持待验收
  - [x] [`M11-11 证据`](phase-3-production-readiness/11-kind-k3d-fault-matrix-evidence.md) 记录完成边界、验证结果、未签署动态 Gate 与不删除 PVC/WAL 的回滚策略
  - [x] 75/82 已完成、7 未完成，下一工作包 PR-M11-12；M10/M11/Phase 3/HUMAN 与真实 fault Gate 均未提前宣称完成
- [ ] PR-M11-12：完成 ArcMut、stable 与 SLO Phase 3 收口
  - [x] M11-12a owned-value leaf：Common 只读 TopicConfig helper 解除 ArcMut 类型绑定并移除本 crate `sync_unsafe_cell`；Remoting `RpcResponse` header 改为独占 `Box`，删除无效 shared-ref mutation facade
  - [x] M11-12b Controller config owner：`ArcSwap` 不可变快照与串行 copy/validate/publish 写入替代全部 `ArcMut<ControllerConfig>`；失败保持 last-known-good，旧 reader 保持旧快照
  - [x] M11-12c Controller manager/heartbeat lifecycle owner：Manager 根对象改为 `Arc`，initialize/start/shutdown 串行；heartbeat 生命周期内部同步；request processor 与 housekeeping 使用 `Weak` 断开服务图强引用环
  - [x] M11-12d Controller Raft owner：OpenRaft lifecycle 由异步 transition lock 与短临界区状态锁串行，`RaftController`/Manager/Processor 全链路改用安全 `Arc`，共享启动/关闭幂等且不跨 `.await` 持有同步锁
  - [x] M11-12e Controller request processor owner：13 个业务 handler 收窄为共享 receiver，wrapper payload 改为 `Arc`；remoting trait 的 `&mut self` 仅保留为无可变 capability 的兼容适配
  - [x] M11-12f NameServer runtime/processor owner：根对象改为安全 `Arc`，配置以串行 copy/validate/publish 的 `ArcSwap` 不可变快照发布；route/KV/housekeeping/batch/processor 使用 `Weak` handle 断开服务图强引用环
  - [x] M11-12g Remoting Channel/Context owner：`Connection` 只通过 cloneable lifecycle handle 暴露状态，以异步 Mutex 串行唯一 writer；`ChannelInner`、legacy response table 与 handler context 改用安全 `Arc`/显式同步，删除共享引用取得 socket/channel 可变引用的入口
  - [x] M11-12h Remoting client/handler owner：handler 以安全 `Arc` 共享、每请求 clone-local processor adapter 并以短 `RwLock` 快照管理 hooks；client lifecycle 改为标准 `Arc`/`Weak`，NameServer 选择状态显式同步，shutdown/hook/health capability 收窄为共享引用
  - [x] M11-12i NameServer V1 tables：六张 `ArcMut<HashMap>` 改为 manager 独占普通 `HashMap`，由既有 `Mutex<RouteInfoManager>` wrapper 提供唯一写边界；所有变更入口恢复 `&mut self`，删除冗余内部锁与全部可变逃逸
  - [x] M11-12j Remoting protocol compatibility：删除固定 `ArcMut` header/mapping-detail facade，topic-config wire DTO 直接 re-export Protocol canonical `HashMap` owner；Broker/NameServer 构造端发布 owned protocol value
  - [x] M11-12k Client ProduceAccumulator owner：Manager/Producer 改用安全 `Arc`；批大小/延时配置使用原子状态，sync/async guard task handle 与 schedule sender 收入显式 lifecycle mutex；异步关闭先取出 handle 再 await
  - [x] M11-12l Client latency fault detector：trait/strategy/filter/task capture 改用安全 `Arc`；detector 配置原子发布，resolver/service detector 使用短 `RwLock` 的 `Arc` 快照，单一 lifecycle mutex 串行 task 发布/关闭并排除 shutdown 期间 restart
  - [x] M11-12m Client message ownership：`PullResult` 直接持有 owned `MessageExt`，ProcessQueue/consume request/hook/trace/Lite zero-copy 使用标准 `Arc<MessageExt>`；retry/namespace mutation 以 clone-on-write 隔离，消费开始时间由 ProcessQueue 生命周期状态跟踪
  - [x] M11-12n Client consume service lifecycle：通用分发器与四类 Push/Pop concurrent/orderly service 改用标准 `Arc`/`Weak` owner；start/shutdown/request task 只经 `&self`，Pop orderly lock-refresh handle 以 lifecycle mutex 发布并在 await 前取出
  - [x] M11-12o Client send hook/trace context owner：异步 after-hook 只持有不可变 hook 快照，`SendMessageContext` 删除 Producer owner；trace dispatcher 只保存启动后解析出的 client id，不再持有 host Producer/Consumer 实现
  - [x] M11-12p Client Admin facade self owner：Client 与 admin-core facade 直接拥有实现和单一 ClientConfig；ClientInstance Admin group 注册值收窄为 owner-free marker，删除无读取用途的 self `ArcMut` 保活环，Tools production 债务清零
  - [x] M11-12q Client Producer fault strategy owner：Producer 直接拥有策略，异步发送回调克隆阈值快照并只共享 detector/原子开关，删除 `ArcMut<MQFaultStrategy>`
  - [x] M11-12r Client API factory owner：factory client 列表和名称服务器刷新任务改用普通 `Arc<MQClientAPIImpl>`；API client 的名称服务器地址缓存以异步 `RwLock` 串行更新，lifecycle/address capability 收窄为 `&self`
  - [x] M11-12s Client API instance owner：`MQClientInstance` 与 Admin/Producer/Consumer 调用链改持普通 `Arc<MQClientAPIImpl>`；纯转发 receiver 收窄为 `&self`，query/pull task 捕获 `Arc<Self>`，删除 API heartbeat `mut_from_ref`
  - [x] M11-12t Client internal Admin owner：`MQClientInstance` 改持普通 `Arc<MQAdminImpl>`，client handle 以 `OnceLock` 一次绑定，Admin receiver 收窄为 `&self`，删除 Producer Admin-only `mut_from_ref`
  - [x] M11-12u Client route registry owner：route refresh/application、route query、broker lookup 与 Producer 注册入口收窄为 `&self`，Producer 路由/heartbeat/注册路径删除 4 个 safe `mut_from_ref`，仅保留 lifecycle start 可变入口
  - [x] M11-12v Client OffsetStore owner：Push/Lite facade、实现、rebalance 与 callback 改持普通 `Arc<OffsetStore>`；Remote/Local persistence receiver 收窄为 `&self`，Local task handle 以 lifecycle mutex 串行并在 await 前取出
  - [x] M11-12w Client accumulator batch producer owner：`MessageAccumulation` 直接持有 owned `DefaultMQProducer` clone；flush 在 batch mutex 内克隆、锁外发送，删除 accumulator 文件全部 ArcMut 构造、类型与 import
  - [x] M11-12x Client remote offset read access：`RemoteBrokerOffsetStore` 的 broker lookup、route refresh 与 client API 读取直接使用 immutable `MQClientInstance` access，删除 4 个过时 `mut_from_ref`
  - [x] M11-12y Client Push operational access：pull/pop dispatch、retry namespace reset、POP ack/change-invisible receiver 收窄为 `&self`，RebalancePush heartbeat/dispatch 与 consume service 删除 9 个过时 `mut_from_ref`
  - [x] M11-12z Client orderly lock access：Rebalance lock/unlock capability 与 Push/Lite/inner 实现收窄为 `&self`，orderly lock 路径删除 3 个 `mut_from_ref` 并改用 immutable namespace resolution
  - [x] M11-12aa Client Lite Pull config snapshots：实现与 rebalance 配置改用 `ArcSwap` copy-update-publish，不再通过共享引用写配置；兼容 facade 入口保持，内部配置读取只观察完整代际
  - [x] M11-12ab Client Lite Pull facade config snapshots：facade 配置 owner 改为共享 `ArcSwap`，公开 getter 返回 immutable owned `Arc` snapshot，构造边界接收 owned config；builder 与 facade 配置 API 不再暴露 `ArcMut`
  - [x] M11-12ac Client Lite Pull root lifecycle：facade、consumer inner、callback 与 task root 改用标准 `Arc`/`Weak`；专用异步 lifecycle mutex 串行 start/shutdown/订阅控制面，组件槽位使用短锁快照；Rebalance offset store 改为 `ArcSwapOption`
  - [x] M11-12ad Client PullAPIWrapper immutable access：Lite/Push wrapper owner 改用标准 `Arc`；运行参数使用原子发布、filter hook 使用 `ArcSwap` 整代快照，pull/POP/filter-server receiver 收窄为 `&self`
  - [x] M11-12ae Client Push message listener ownership：facade config、implementation 与 Java-compatible getter/setter 改持标准 `Arc<MessageListener>`，concurrent/orderly 注册与替换不再传播共享可变 wrapper
  - [x] M11-12af Client Push subscription snapshots：deprecated startup subscription map 改持标准 `Arc<HashMap>`，config/builder/Java-compatible getter/setter 返回 immutable owned snapshot；dynamic rebalance table 不变
  - [x] M11-12ag Client Push consume service config snapshots：concurrent/orderly 与 POP concurrent/orderly 服务持有同一启动代的 immutable `Arc<ClientConfig>`/`Arc<ConsumerConfig>`，服务不再暴露配置共享写入口
  - [x] M11-12ah Client Push rebalance config snapshots：RebalancePush 使用 `ArcSwap<ConsumerConfig>` 发布完整不可变代际；相关 facade setter 显式同步，队列数变化只通过 Push implementation owner 回写两个动态 threshold
  - [x] M11-12ai Client Push root config snapshots：facade 与 implementation 共享 `Arc<ArcSwap<ConsumerConfig>>`，setter 以 clone-update-publish 发布完整代际；启动、回调、diagnostics 与动态 threshold 更新只读取稳定 immutable `Arc` 快照
  - [x] M11-12aj Client Push implementation root ownership：facade、consumer registry、callback 与 task capture 改用标准 `Arc`，root-owned consume/rebalance 回边改用 `Weak`；start/shutdown 由 lifecycle mutex 串行，组件槽位短锁发布快照，rebalance metadata 使用共享引用安全更新且未新增 production `mut_from_ref`
  - [x] M11-12ak Client Rebalance root ownership：Push/LitePull concrete rebalance root 改用标准 `Arc`，core self-reference 与 concrete setter 改用标准 `Weak`；释放 root 后 weak upgrade 失败的定向测试通过，`MQClientInstance` 兼容 handle 保留给后续切片
  - [x] M11-12al Client MQClientInstance root ownership：Manager/Proxy handle 与 Admin/Producer/Consumer/Rebalance/API/OffsetStore 全链路改用标准 `Arc<MQClientInstance>`；Remoting/Admin 回指改用标准 `Weak`，lifecycle、API slot 与 task handle 显式同步，公开运行路径收窄为共享 receiver
  - [x] M11-12am Client internal child ownership：`MQClientInstance` 的 PullMessageService child 改用标准 `Arc`，internal DefaultMQProducer 改由单一异步 `Mutex` 所有并取代冗余 transition lock；production factory 文件不再包含 ArcMut
  - [x] M11-12an Client Producer root ownership：DefaultMQProducer facade/implementation/registry 改用标准 `Arc`/`Weak`；单一 runtime snapshot、短锁配置发布、异步 lifecycle、task admission 与 owner-aware unregister 替代共享可变 root，Client production ArcMut 清零
  - [x] M11-12ao Broker topic metadata table ownership：TopicRouteInfoManager 四张共享表改用标准 `Arc<DashMap>`，TopicQueueMappingManager 改用不可变标准 `Arc` 整值代际；读 guard 在同表写入或异步边界前释放，cleanup 以 observed Arc identity 做条件发布，旧 mapping 代际在替换后保持有效且不会覆盖并发新代际
  - [x] M11-12ap Broker topic configuration ownership：TopicConfig 表值、快照和 Store carrier 改用不可变标准 `Arc` 代际；TopicConfig 与 DataVersion 在单一 metadata transition 中提交，注册发送共用异步顺序锁并在取锁后重采样，持久化/从节点替换发布一致表与版本
  - [x] M11-12aq Broker POP buffer ownership：`PopBufferMergeService` 与 checkpoint wrapper 改用标准 `Arc`，扫描任务独占复用 ACK scratch，服务 API 收窄为 `&self`；扫描在异步 I/O 前释放 DashMap guard，以 observed Arc identity 条件删除旧代际，并保留 commit-offset FIFO 直至按序提交
  - [x] M11-12ar Broker POP lifecycle ownership：`PopMessageProcessor`/`NotificationProcessor` root 与长轮询 service 改用标准 `Arc`，processor/service 与 service/scan-task 回边改为标准 `Weak`；共享 wake-up receiver、原子 cleanup 时间和异步 lifecycle gate 消除别名可变访问并串行 start/shutdown/restart
  - [x] M11-12as Broker POP Lite lifecycle ownership：`PopLiteMessageProcessor`/`PopLiteLongPollingService` root 与 Broker processor/runtime carrier 改用标准 `Arc`，processor 回边和 scan task 改用标准 `Weak`；共享 wake-up trait、每次 start 的新 channel、停止状态轮询拒绝与双层异步 lifecycle gate 消除共享可变别名并支持安全重启
  - [x] M11-12at Broker Pull lifecycle ownership：`PullMessageProcessor`/result handler/request-hold service 与 Broker carrier 改用标准 `Arc`，hold service 与 scan task 改用标准 `Weak`；共享 processor 能力、异步 lifecycle gate、停止准入/清理与锁内 deadline 发布消除强引用环、共享可变别名和 start/shutdown/deadline 竞态
  - [x] [`M11-12 进度证据`](phase-3-production-readiness/11-soundness-closure-progress.md) 记录父 Issue #8292、子切片 Issue #8293/#8295/#8297/#8299/#8301/#8303/#8307/#8309/#8311/#8313/#8315/#8317/#8319/#8321/#8323/#8325/#8327/#8329/#8331/#8333/#8335/#8337/#8339/#8341/#8343/#8345/#8347/#8349/#8351/#8353/#8355/#8357/#8359/#8361/#8363/#8365/#8367/#8369/#8371/#8375/#8377/#8379/#8381/#8383/#8385/#8387 与每次真实下降
  - [x] Issue #8295 后累计降至 711 production/2,029 occurrence；Controller 配置债务清零但其他 Controller owner 仍有 31 条 production 债务
  - [x] Issue #8297 后实际快照降至 697 production/1,986 occurrence；Controller 降至 17 条/51 occurrence，Manager/heartbeat/embedded-NameServer owner 已退出 `ArcMut`
  - [x] Issue #8299 后实际快照降至 690 production/1,961 occurrence；Controller 降至 10 条/26 occurrence，Raft/OpenRaft owner 与 Manager Raft `mut_from_ref` 已清零
  - [x] Issue #8301 后实际快照降至 688 production/1,959 occurrence；Controller 降至 8 条/24 occurrence，request processor wrapper 已退出 `ArcMut`
  - [x] Issue #8303 后实际快照降至 669 production/1,918 occurrence；NameServer 降至 28 条/58 occurrence（V1 tables 16/44、remoting client 4/5、Context 8/9），runtime/KV/V2 route/batch/housekeeping/request-processor owner 已退出 `ArcMut`
  - [x] Issue #8305 按实际快照校正 NameServer 子类别分配；28 条/58 occurrence 总量与 reviewed baseline 不变
  - [x] Issue #8307 后实际快照降至 514 production/1,612 occurrence；Remoting Channel/Context 债务清零，Auth/Proxy 的 Context 传播债务同步清零，NameServer 降至 20/49、Controller 降至 4/6
  - [x] Issue #8309 后实际快照降至 488 production/1,559 occurrence；Remoting client/handler owner 清零，Controller production 债务清零，NameServer 仅剩 V1 tables 16/44，Remoting 仅剩 protocol compatibility 6/9
  - [x] Issue #8311 后实际快照降至 472 production/1,515 occurrence；NameServer production 债务清零，reviewed baseline 仅删除 V1 tables 的 16 条/44 occurrence，无 relocation
  - [x] Issue #8313 后实际快照降至 466 production/1,505 occurrence；Remoting production 债务清零，Broker wire-wrapper occurrence 同步减少 1 次
  - [x] Issue #8315 后实际快照降至 463 production/1,495 occurrence；Client 降至 143/589，ProduceAccumulator 共享 owner 退出 `ArcMut`
  - [x] Issue #8317 后实际快照降至 454 production/1,481 occurrence；Client 降至 134/575，latency fault detector production 债务清零
  - [x] Issue #8319 后实际快照降至 440 production/1,397 occurrence；Client 降至 120/491，production 中 `ArcMut<MessageExt>` 消息流清零
  - [x] Issue #8321 后实际快照降至 436 production/1,337 occurrence；Client 降至 116/431，consume service lifecycle owner 与 task capture 退出 `ArcMut`
  - [x] Issue #8323 后实际快照降至 432 production/1,329 occurrence；Client 降至 112/423，send hook/trace context 不再反向持有 Producer/Consumer owner
  - [x] Issue #8325 后实际快照降至 424 production/1,295 occurrence；Client 降至 107/403、Tools production 清零，Admin facade/config/registration self owner 退出 `ArcMut`
  - [x] Issue #8327 后实际快照降至 423 production/1,292 occurrence；Client 降至 106/400，Producer fault strategy owner 退出 `ArcMut`
  - [x] Issue #8329 后实际快照降至 421 production/1,286 occurrence；Client 降至 104/394，API factory owner 与名称服务器刷新任务退出 `ArcMut`
  - [x] Issue #8331 后实际快照降至 420 production/1,276 occurrence；Client 降至 103/384，API instance owner、纯转发可变 receiver 与 heartbeat `mut_from_ref` 退出共享边界
  - [x] Issue #8333 后 production 条目保持 420，occurrence 降至 1,263；Client 为 103/371，internal Admin owner 与 11 个 Producer Admin-only `mut_from_ref` 退出共享边界
  - [x] Issue #8335 后 production 条目保持 420，occurrence 降至 1,259；Client 为 103/367，Producer route/heartbeat/registration 的 4 个 `mut_from_ref` 退出共享边界，仅余 lifecycle start
  - [x] Issue #8337 后实际快照降至 418 production/1,224 occurrence；Client 降至 101/332，Push/Lite OffsetStore owner 与 7 个 offset persistence/shutdown `mut_from_ref` 退出共享边界
  - [x] Issue #8339 后实际快照降至 415 production/1,219 occurrence；Client 降至 98/327，ProduceAccumulator production/test ArcMut 债务清零
  - [x] Issue #8341 后实际快照降至 414 production/1,215 occurrence；Client 降至 97/323，RemoteBrokerOffsetStore 只读查询路径 `mut_from_ref` 清零
  - [x] Issue #8343 后实际快照降至 411 production/1,206 occurrence；Client 降至 94/314，Push request/POP API/retry reset 只读访问删除 3 个 identity、9 个 occurrence
  - [x] Issue #8345 后实际快照降至 410 production/1,203 occurrence；Client 降至 93/311，orderly service `mut_from_ref` 清零，POP-orderly 仅保留 producer send 可变入口
  - [x] Issue #8347 后 production identity 保持 410，occurrence 降至 1,169；Client 为 93/277，Lite Pull 实现与 rebalance 配置 owner 的 34 个共享可变 occurrence 退出
  - [x] Issue #8349 后实际快照降至 408 production/1,129 occurrence；Client 降至 91/237，Lite Pull facade config 与 builder 的 2 个 production identity、40 个 occurrence 退出
  - [x] Issue #8351 后实际快照降至 402 production/1,102 occurrence；Client crate 降至 85/210，Lite Pull root lifecycle 的 6 个 production identity、27 个 production occurrence 与 5 个 test identity、15 个 test occurrence 退出
  - [x] Issue #8353 后 production identity 保持 402、occurrence 降至 1,095；Client 为 85/203，PullAPIWrapper 的 7 个 production occurrence 与 1 个 test occurrence 退出共享可变边界
  - [x] Issue #8355 后 production identity 保持 402、occurrence 降至 1,086；Client 为 85/194，Push MessageListener 的 9 个 production occurrence 与 3 个 test occurrence 退出，1 个 test identity 删除
  - [x] Issue #8357 后实际快照降至 400 production/1,078 occurrence；Client 降至 83/186，Push startup subscription snapshot 的 2 个 production identity/8 occurrence 与 1 个 test identity/1 occurrence 退出
  - [x] Issue #8359 后实际快照降至 398 production/1,054 occurrence；Client owner 降至 80/161，另有 Proxy 1/1；四类 Push consume service 配置的 2 个 production identity/24 occurrence 与 16 个 test occurrence 退出
  - [x] Issue #8361 后 production identity 保持 398、occurrence 降至 1,052；Client owner 降至 80/159，RebalancePush 的 2 个 `ArcMut<ConsumerConfig>` occurrence 退出且测试/compatibility 不增
  - [x] Issue #8363 后实际快照降至 397 production/1,045 occurrence；Client owner 降至 79/152，测试从 47/145 降至 47/132；Push 根 `ConsumerConfig` 的 1 个 identity/20 occurrence 退出
  - [x] Issue #8365 后实际快照降至 376 production/995 occurrence；Client owner 降至 58/102，Client test 降至 25/102；Push implementation root、consume service/callback owner 与过时 rebalance 可变 receiver 共删除 21 个 production identity/50 occurrence、22 个 test identity/30 occurrence
  - [x] Issue #8367 后实际快照降至 368 production/982 occurrence；Client owner 降至 50/89；Push/LitePull Rebalance root 与 standard-weak self reference 共删除 8 个 production identity/13 occurrence，测试/compatibility 不增
  - [x] Issue #8369 后实际快照降至 326 production/909 occurrence；Client owner 降至 9/17、Client test 降至 12/83、Proxy production 清零；MQClientInstance root 全链路共删除 42 个 production identity/73 occurrence 与 13 个 test identity/19 occurrence
  - [x] Issue #8371 后实际快照降至 323 production/904 occurrence；Client owner 降至 6/12，Client test 12/83 与 compatibility 14/40 不增；factory child owner 删除 3 个 production identity/5 occurrence
  - [x] Issue #8375 后实际快照降至 317 production/892 occurrence、196 test/559 occurrence；Client production 清零，Client test 降至 4/71，Producer root 删除 6 个 production identity/12 occurrence 与 8 个 test identity/12 occurrence
  - [x] Issue #8377 后实际快照降至 312 production/873 occurrence、194 test/551 occurrence；Broker 降至 185/549，topic route/queue mapping 表 owner 删除 5 个 production identity/19 occurrence 与 2 个 test identity/8 occurrence
  - [x] Issue #8379 后实际快照降至 300 production/783 occurrence、168 test/466 occurrence；Broker 降至 178/475、Store 降至 122/308，TopicConfig value/DataVersion owner 共删除 12 个 production identity/90 occurrence 与 26 个 test identity/85 occurrence，compatibility 14/40 不增
  - [x] Issue #8381 后实际快照降至 298 production/764 occurrence、167 test/464 occurrence；Broker 降至 176/456，POP buffer/checkpoint owner 共删除 2 个 production identity/19 occurrence 与 1 个 test identity/2 occurrence，compatibility 14/40 不增
  - [x] Issue #8383 后实际快照降至 296 production/738 occurrence、166 test/463 occurrence；Broker 降至 174/430，POP/Notification lifecycle 共删除 2 个 production identity/26 occurrence 与 1 个 test identity/1 occurrence，compatibility 14/40 不增
  - [x] Issue #8385 后实际快照降至 294 production/725 occurrence、165 test/462 occurrence；Broker 降至 172/417，POP Lite lifecycle 共删除 2 个 production identity/13 occurrence 与 1 个 test identity/1 occurrence，compatibility 14/40 不增
  - [x] Issue #8387 后实际快照降至 289 production/706 occurrence、162 test/458 occurrence；Broker 降至 167/398，Pull lifecycle 共删除 5 个 production identity/19 occurrence 与 3 个 test identity/4 occurrence，compatibility 14/40 不增
  - [ ] M11-12au 及后续：Broker ConsumerOffsetManager/root/schedule/其他 processor/transaction、Store/HA、compatibility 删除、stable/Miri/Loom/soak/SLO 与同一候选快照 Gate 仍待完成
  - [ ] 总进度仍为 75/82；本子切片不提前计作完成工作包，M10/Kind-K3d/container dynamic/HUMAN Gate 保持开放
- [ ] 对应任务文档的 Exit Checklist 全部通过

### Phase 3 Gate

- [ ] CommitLog 是唯一权威 WAL，派生引擎只持久 cursor/watermark
- [ ] dirty-tail、flush、replay、generation rollback 与故障注入通过
- [ ] 固定 profile 下性能、p99、RSS 和 I/O amplification 达到门槛
- [ ] production/public compatibility API 无不安全 ArcMut 逃逸
- [ ] secure profile、secret reload、telemetry semantics、镜像与滚动升级 e2e 通过
- [ ] SLO、dashboard、runbook 和 rollback 步骤与代码同步
- [ ] `[ARCH]`、`[REV]`、`[TEST]` 已签署
- [ ] `[HUMAN]` 已批准进入 Phase 4

## 6. Phase 4：AI Native 运维

### M12 AI Native 证据驱动运维

任务文档：[`12-ai-native-operations.md`](phase-4-ai-native/12-ai-native-operations.md)

- [ ] PR-M12-01：实现 Evidence normalization 与 Knowledge Graph
- [ ] PR-M12-02：实现受控 RAG
- [ ] PR-M12-03：实现多领域确定性诊断
- [ ] PR-M12-04：冻结 Plan contract 并证明无副作用
- [ ] `[HUMAN]` 已单独决定是否实施 Apply
- [ ] PR-M12-05：实现独立 Apply 边界（仅在批准后）
- [ ] PR-M12-06：完成 eval、red-team 与离线 fallback
- [ ] 对应任务文档的 Exit Checklist 全部通过

### Phase 4 Gate

- [ ] AI/LLM 不在 Broker、Client、Store 数据路径
- [ ] KG/RAG 满足 tenant、source、freshness、privacy 和有界资源要求
- [ ] 确定性规则可重放并正确标记 partial/missing evidence
- [ ] 现有 Plan Tool 无副作用合同全部通过
- [ ] 若 Apply 存在，compile/runtime/RBAC/approval 门禁及 audit/verify/rollback 均 fail closed
- [ ] LLM 离线不影响核心服务和人工 CLI/API
- [ ] threat model/red-team 无未解决高风险
- [ ] `[ARCH]`、`[REV]`、`[TEST]`、`[HUMAN]` 已签署最终目标态 Gate

## 7. 每次交付完成记录模板

> 每完成一个 PR 复制一次本节；不要预先勾选。`<...>` 必须替换为实际值。

### `<PR-ID>`：`<标题>`

| 字段 | 值 |
|---|---|
| 里程碑 | `<Mxx>` |
| 状态 | `待验收` |
| 基线提交 | `<commit>` |
| 候选提交/快照 | `<commit-or-snapshot>` |
| 完成日期 | `<YYYY-MM-DD>` |
| Developer | `<name>` |
| Reviewer | `<name>` |
| Tester | `<name>` |
| Evidence Index | `target/architecture-refactor/<Mxx>/<run-id>/` |

#### 范围与实现

- [ ] 实际变更与任务文档的 PR 范围一致
- [ ] 已检查 `git status --short`，未覆盖或重格式化无关用户改动
- [ ] 行为修复、机械迁移、公开 API/feature 变化已拆分或明确说明
- [ ] 新增/修改的公开 API、wire、storage、Serde、feature 兼容影响已记录
- [ ] owner、资源预算、shutdown、错误传播和回滚不变量已满足
- [ ] 已添加能在变更前失败、变更后通过的聚焦测试（行为变更适用）

#### 验证证据

| 命令 | 触发原因 | 退出码 | 结果 | 证据路径/Hash |
|---|---|---:|---|---|
| `<focused test>` | `<behavior>` | `<0/non-zero>` | `<通过/失败/未执行>` | `<path/hash>` |
| `<package check/test>` | `<crate>` | `<0/non-zero>` | `<通过/失败/未执行>` | `<path/hash>` |
| `<feature/consumer validation>` | `<feature/consumer>` | `<0/non-zero>` | `<通过/失败/未执行>` | `<path/hash>` |
| `<specialized guard>` | `<trigger>` | `<0/non-zero>` | `<通过/失败/未执行>` | `<path/hash>` |
| `cargo fmt --all -- --check` | Rust 最终格式门禁 | `<0/non-zero>` | `<通过/失败/不适用>` | `<path/hash>` |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 根 workspace Rust 最终门禁 | `<0/non-zero>` | `<通过/失败/不适用>` | `<path/hash>` |
| `git diff --check` | whitespace 检查 | `<0/non-zero>` | `<通过/失败>` | `<path/hash>` |

- [ ] 每个必需命令均记录了完整命令、工具链、退出码和输出位置
- [ ] 已按变更范围执行所有 standalone consumer 与 specialized gate
- [ ] 失败或跳过项写明原因、基线证据和剩余风险
- [ ] 性能测试记录硬件、内核、文件系统、profile、feature、消息大小、TLS 比例和采样方法（适用）

#### 审查、回滚与交接

- [ ] `[REV]` 已在冻结候选快照上完成独立审查
- [ ] `[TEST]` 已在同一候选快照上完成独立验证
- [ ] 审查/测试后若发生修复，受影响结论已对新快照重跑
- [ ] 回滚触发器、回滚步骤和兼容数据处理已验证
- [ ] evidence index、fixture/golden、ADR/API diff/runbook 已更新
- [ ] 下一 PR/里程碑的输入、已知风险和阻塞项已写入 handoff

#### 完成结论

- [ ] Developer 声明实现与证据齐全
- [ ] Reviewer 结论：`通过`
- [ ] Tester 结论：`通过`
- [ ] Human Architect 批准兼容/安全/架构决策（适用）
- [ ] 对应里程碑 PR 项已在本文件勾选

```text
status: completed | blocked
summary: <本次完成内容和结果>
artifacts:
  - <变更文件或交付物>
  - <evidence index>
validation:
  - command: <完整命令>
    exit_code: <退出码>
    result: <通过/失败/未执行及原因>
rollback: <回滚点与验证结果>
next_actions:
  - <下一任务>
stop_reason: <仅 blocked 时填写>
```

## 8. 最终目标态 Checklist

- [ ] 目标 32-package DAG、10 个新 crate 边界和 Client allowlist 可由 CI 证明
- [ ] ArcMut 不安全共享可变逃逸退出 production/public compatibility API
- [ ] durability、bounded lifecycle、兼容性和故障恢复均有自动化证据
- [ ] 性能绝对目标附固定硬件 profile、资源预算和可重复报告
- [ ] secure cloud deployment、SLO、dashboard、runbook 与回滚演练通过
- [ ] AI 证据链、Plan/Apply 边界、离线 fallback 和 red-team 通过
- [ ] 设计文档中 96/100 的每个评分维度均链接到可复现证据
- [ ] 未达到的门槛未被计入代码现状分
