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
| Phase 2 | M04–M09 | 进行中 | Codex 多代理执行组 | 12–16 周 | — | [`phase-2-core-boundaries/`](phase-2-core-boundaries/) |
| Phase 3 | M10–M11 | 未开始 | 待分配 | 8–12 周 | — | — |
| Phase 4 | M12 | 未开始 | 待分配 | 8–12 周 | — | — |

### 2.1 剩余重构盘点（2026-07-16）

> 统计口径：只统计 82 个顶层 `PR-Mxx-yy` 工作包；M06-03a～ah 等内部迁移证据不重复计数。

| 指标 | 已完成 | 进行中 | 未开始/未完成 | 目标 |
|---|---:|---:|---:|---:|
| PR 级工作包 | 30 | 1（PR-M06-03） | 51 未开始；合计 52 尚未完成 | 82 |
| 里程碑 | 5（M01–M05） | 1（M06） | 6（M07–M12） | 12 |
| 新增边界 crate | 6 | 0 | 4（store-rocksdb、proxy-core/cluster/local） | 10 |
| 根 workspace package | 28 | — | 还差 4 | 32 |
| Phase Gate | 1 | 1（Phase 2） | 2（Phase 3、Phase 4） | 4 |

剩余 51 个未开始工作包分布：M06-04～12 为 9 个、M07 为 7 个、M08 为 6 个、M09 为 6 个、
M10 为 5 个、M11 为 12 个、M12 为 6 个。当前 M06-03 仍需完成 CommitLog 根结构、MappedFileQueue I/O/allocate adapter 及剩余算法、
append/recovery 方法 owner 与 facade 收口，因此尚未勾选父项。

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
- [ ] PR-M06-03：创建 Local crate 并迁移 CommitLog/load/recovery
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
- [ ] PR-M06-04：机械迁移 Flush 与 Group Commit
- [ ] PR-M06-05：迁移 CQ 与 Index
- [ ] PR-M06-06：迁移 HA、Replication 与 Transfer
- [ ] PR-M06-07：迁移 Timer、POP 与 Local Services
- [ ] PR-M06-08：收敛 LocalFileMessageStore facade、composition 与 config
- [ ] PR-M06-09：创建 RocksDB foundation
- [ ] PR-M06-10：实现 RocksDB MessageStore adapter 与 parity
- [ ] PR-M06-11：完成 Store facade、Tiered 反转与 feature 所有权
- [ ] PR-M06-12：完成依赖图与消费方收口
- [ ] 对应任务文档的 Exit Checklist 全部通过

### M07 Legacy Runtime 排空与 Client 依赖边收敛

任务文档：[`07-legacy-and-client-edge-burn-down.md`](phase-2-core-boundaries/07-legacy-and-client-edge-burn-down.md)

- [ ] PR-M07-01：将 `rocketmq-rust` 生命周期能力迁入 runtime
- [ ] PR-M07-02：删除 MCP 冗余 Client 边
- [ ] PR-M07-03：完成 NameServer RouteLookup 反转
- [ ] PR-M07-04：清零 Broker Client 边
- [ ] PR-M07-05：收敛 Admin contract 与 Client adapter
- [ ] PR-M07-06：迁移 Web/Tauri Dashboard
- [ ] PR-M07-07：完成 allowlist 与 consumer closeout
- [ ] 对应任务文档的 Exit Checklist 全部通过

### M08 Proxy Core、Cluster、Local 三向物理拆分

任务文档：[`08-proxy-three-way-split.md`](phase-2-core-boundaries/08-proxy-three-way-split.md)

- [ ] PR-M08-01：创建 `rocketmq-proxy-core` 与 proto owner
- [ ] PR-M08-02：迁移中立 plan、port、service 与 ingress
- [ ] PR-M08-03：创建 Cluster adapter
- [ ] PR-M08-04：创建 Local adapter
- [ ] PR-M08-05：将现有 Proxy 降为 composition/facade
- [ ] PR-M08-06：验证 feature closure 与下一 major fixture
- [ ] 对应任务文档的 Exit Checklist 全部通过

### M09 Facade 收口与 32-Package Gate

任务文档：[`09-facade-and-package-closeout.md`](phase-2-core-boundaries/09-facade-and-package-closeout.md)

- [ ] PR-M09-01：收口 workspace 与目标 DAG
- [ ] PR-M09-02：完成 facade 与 legacy purity 审查
- [ ] PR-M09-03：证明 public API、feature、wire/storage 兼容
- [ ] PR-M09-04：验证 Client allowlist 与跨项目消费者
- [ ] PR-M09-05：准备 R0/R1/下一 major 发布包
- [ ] PR-M09-06：冻结快照并执行 Phase 2 Gate
- [ ] 对应任务文档的 Exit Checklist 全部通过

### Phase 2 Gate

- [ ] 根 workspace 精确包含 32 个 package
- [ ] 10 个新 crate 的禁止依赖边为零，目标 DAG 无环
- [ ] 完整 Client 直接消费者收敛为 workspace 2 个、standalone 1 个
- [ ] `proxy-core`/`proxy-local` 的传递闭包不含完整 Client
- [ ] canonical/legacy API、wire、storage、Serde 与 feature 兼容 fixture 全部通过
- [ ] facade/legacy ledger 只下降，所有剩余项有 owner 和退出里程碑
- [ ] `[ARCH]`、`[REV]`、`[TEST]` 已签署
- [ ] `[HUMAN]` 已批准进入 Phase 3

## 5. Phase 3：生产就绪

### M10 耐久派生引擎与可量化性能

任务文档：[`10-durability-and-performance.md`](phase-3-production-readiness/10-durability-and-performance.md)

- [ ] PR-M10-01：建立派生 cursor 合同和 replay harness
- [ ] PR-M10-02：实现 Tiered cursor、retry ledger 与背压
- [ ] PR-M10-03：优化 CQ、Rocks 与 Tiered 读取
- [ ] PR-M10-04：实现 Index/Compaction generation
- [ ] PR-M10-05：完成 benchmark、soak 与性能 Gate
- [ ] 对应任务文档的 Exit Checklist 全部通过

### M11 安全、可观测性与云原生生产化

任务文档：[`11-security-observability-cloud.md`](phase-3-production-readiness/11-security-observability-cloud.md)

- [ ] PR-M11-01：建立 Telemetry semantic registry
- [ ] PR-M11-02：实现 SecretProvider 基础合同与本地 adapter
- [ ] PR-M11-03：实现 Secure Profile 与一次性 bootstrap
- [ ] PR-M11-04：实现 credential/certificate rotation 与原子 reload
- [ ] PR-M11-05：完成 MCP HTTPS、JWKS 与 Principal 传播
- [ ] PR-M11-06：完成 MCP Audit Writer 与 Shutdown Drain
- [ ] PR-M11-07：建立容器镜像基础
- [ ] PR-M11-08：交付五个服务镜像入口
- [ ] PR-M11-09：交付 Helm 与 Kustomize 资产
- [ ] PR-M11-10：统一 Probe、PreStop 与 Drain
- [ ] PR-M11-11：完成 Kind/K3d fault matrix Gate
- [ ] PR-M11-12：完成 ArcMut、stable 与 SLO Phase 3 收口
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
