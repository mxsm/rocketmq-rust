# M06：Store API、Local 与 RocksDB 边界提取

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 已批准，等待 M02/M03 |
| 预计周期 | 4–6 周 |
| 工作包 | WP11 `storage-capability-spike`、WP12 `store-local-extract`、WP13 `store-rocks-extract`；承接 WP02 |
| 前置条件 | flush/watermark 语义稳定；model 查询值可用；storage golden 和 RocksDB baseline 已冻结 |
| 可并行项 | API 设计冻结后，Local 非 CommitLog 模块与 Rocks fixture 可并行准备；实际 owner 移动按 API→Local→Rocks 串行 |
| 完成后解锁 | M07、M10 |

## 目标

- 创建 `rocketmq-store-api`、`rocketmq-store-local`、`rocketmq-store-rocksdb` 三个物理边界。
- 用窄 capability 和 backend-neutral error 替代新调用方对 126 方法 MessageStore 的依赖，旧 trait 保 forwarding adapter。
- CommitLog 只属于 Local 且是唯一 WAL；RocksDB 复用 Local CommitLog，只替换 CQ/Index 派生路径。
- 现有 `rocketmq-store` 长期作为 backend composition、Tiered decorator 和兼容 facade。

## 非目标

- 不把 MappedFile、HA、Timer、RocksDB、Tiered 实现类型暴露到 store-api。
- 不为 RocksDB 创建第二套消息 WAL，不改变 20B CQ/Index 或已持久化格式。
- 不在机械提取时调整 fast/safe 优先级、默认 backend 或 Rocks+Tiered 未定义组合。
- M10 的 Tiered cursor、compaction generation 和性能优化不在本里程碑实现。

## 入口条件

- [ ] `[ARCH]` 冻结 capability、AppendReceipt/DerivedProgress/StoreError 语义和 legacy adapter 边界。
- [ ] `[TEST]` 准备 dirty-tail、flush、HA、recovery、20B CQ/Index、Local/Rocks parity corpus。
- [ ] `[DEV]` 检查 store/broker/tieredstore 目标文件和现有用户修改无重叠。
- [ ] `[HUMAN]` 批准 CommitLog 唯一 WAL 与 `rocks → local → api` 单向关系。

## 交付物

| 类型 | 交付物 |
|---|---|
| API | lifecycle、append/read/index/health/replication/admin/dispatch/progress/error capability |
| Result | AppendReceipt、DerivedProgress、Durability、Bytes/lease 中立读取结果 |
| Local | CommitLog/recovery/flush/MappedFile/CQ/Index/HA/Timer/POP 和 Local store owner |
| Rocks | RocksDB CQ/Index/maintenance/snapshot/message-store adapter，复用 Local CommitLog |
| Facade | backend enum/factory、legacy MessageStore/config/path、Tiered composition |
| Features | 精确 no-default/local/fast/safe/io_uring/rocks/tiered/observability 所有权和 alias |
| Tests | compatibility、golden、fault、parity、dependency closure |

## PR 级开发步骤

### PR-M06-01：Store capability spike

- [ ] `[ARCH]` 固定 StoreLifecycle、MessageAppender、MessageReader、OffsetIndex、StoreHealth、ReplicationControl、DerivedRecordSink、AdminStore。
- [ ] `[DEV]` 创建 `rocketmq-store-api`，继承 workspace 元数据，`default = []`，只依赖 model/error/Bytes 类值库。
- [ ] `[DEV]` 设计 backend-neutral StoreError；旧包含 Rocks/Tiered/HA 细节的错误通过 adapter 映射。
- [ ] `[DEV]` 让一个真实 Broker processor 只依赖 `MessageAppender + StoreHealth`，保留旧 MessageStore adapter。
- [ ] `[TEST]` 比较新旧 processor 输出、错误、writable 和 watermark 语义。
- [ ] `[REV]` 检查 API 无 Tokio/runtime/remoting/observability/MappedFile/native backend 类型。
- [ ] 回滚点：processor 恢复 legacy trait；新 API 可删除，不影响 store 实现。

### PR-M06-02：中立 receipt/read result 与 compatibility bridge

- [ ] `[DEV]` 实现 AppendReceipt 的 first/last/appended/durable watermark 和 Durability。
- [ ] `[DEV]` 实现 DerivedProgress/StoreHealth，明确派生进度不是主写 ack 条件。
- [ ] `[DEV]` 为 Get/Query/SelectMappedBuffer 设计 Bytes/lease 中立结果；旧 MappedFile 结果留 Local adapter。
- [ ] `[DEV]` 将旧 126 方法 trait 组合/转发到窄 capability，不新增 required method。
- [ ] `[TEST]` 覆盖错误映射、lease 生命周期、receipt 等价和 legacy trait compile fixture。
- [ ] `[REV]` 检查热路径用泛型/enum，只有冷边界用 `Arc<dyn Trait>`。
- [ ] 回滚点：保留 API crate但撤销首个 consumer；不复制旧 trait 到新 crate。

### PR-M06-03：创建 Local crate 并迁 CommitLog/load/recovery

- [ ] 入口：`[ARCH]` 确认 M02 `try_flush` 契约和 recovery golden 已冻结；本 PR 不搬 flush/group-commit，也不修改恢复行为。
- [ ] `[DEV]` 创建 `rocketmq-store-local`，`default = []`，拥有 fast-load/safe-load/io_uring。
- [ ] `[DEV]` 机械迁移 CommitLog append/load/recovery、MappedFile 与所需最小 config；store 旧深路径精确 re-export。
- [ ] `[TEST]` focused test：dirty-tail truncate、CRC、segment roll、load/recovery 和 crash-before-flush golden。
- [ ] `[REV]` 检查唯一 CommitLog owner、文件格式不变，Local 不依赖 Rocks/Tiered/store facade/Broker/remoting。
- [ ] 回滚点：store facade factory 指回原 CommitLog/load/recovery 实现；旧 public path 与磁盘数据不变。

### PR-M06-04：机械迁移 Flush 与 Group Commit

- [ ] 入口：`[TEST]` M02 的 `try_flush`、legacy adapter 和 SyncFlush/ack 契约测试全部通过；任何行为缺陷先回 M02 修复。
- [ ] `[DEV]` 只迁移 flush manager、group-commit request/worker 和 checkpoint 接线；canonical `try_flush` 与 R0 `flush() -> i64` adapter 语义保持不变。
- [ ] `[DEV]` 所有 SyncFlush/ack 继续只调用 `try_flush`；legacy adapter 留 facade，不作为内部确认入口。
- [ ] `[TEST]` focused test：I/O failure、同批 waiter、group-commit batching、watermark 单调性和 crash/restart。
- [ ] `[REV]` 以机械迁移 diff 审查，没有顺带调整 batch 阈值、fsync 策略、错误分类或默认配置。
- [ ] 回滚点：flush delegation 指回迁移前模块；不得回滚 M02 正确性契约或恢复 `i64` ack 判定。

### PR-M06-05：迁移 CQ 与 Index

- [ ] 入口：`[ARCH]` 20B CQ、Index header/slot、offset 与 Java compatibility golden 已签名。
- [ ] `[DEV]` 机械迁移 consume_queue/queue/index 和必要 message encoder adapter，保持 dispatch 顺序和文件路径。
- [ ] `[TEST]` focused test：20B unit golden、min/max offset、replay、index query、dirty tail和边界 offset。
- [ ] `[REV]` 检查没有引入新的分配/I/O 优化；性能改动留 M10，行为修复单独 PR。
- [ ] 回滚点：CQ/Index factory 分别指回旧实现；CommitLog owner和已写格式不变。

### PR-M06-06：迁移 HA、Replication 与 Transfer

- [ ] 入口：`[ARCH]` replication capability、ack 条件、leader/follower recovery 和 transfer wire 语义已冻结。
- [ ] `[DEV]` 机械迁移 HA、replication、transfer 和相关 checkpoint adapter；background work 继续由 ServiceContext/TaskGroup 拥有。
- [ ] `[TEST]` focused test：HA handshake/offset、replica catch-up、leader restart、transfer partial write和shutdown deadline。
- [ ] `[REV]` 检查没有第二 WAL、没有 detached thread/task，HA 状态不泄漏到 store-api。
- [ ] 回滚点：composition 选择旧 HA/transfer adapter；不回滚 CommitLog/flush owner或持久数据。

### PR-M06-07：迁移 Timer、POP 与 Local Services

- [ ] 入口：`[TEST]` Timer/POP/revive/cold-data/stats 当前行为和 feature fixture可重复。
- [ ] `[DEV]` 分批机械迁移 timer、pop、services、stats、hook/filter adapter；每批使用独立提交。
- [ ] `[TEST]` focused test：timer recovery/expiry、POP checkpoint/revive、cold-data check、service start/stop和feature gates。
- [ ] `[REV]` 检查 owner/task/budget 不变，未把 Broker 私有状态或 façade 依赖带入 Local。
- [ ] 回滚点：按 Timer、POP、services 三个 delegation 独立切回；已迁其他模块不受影响。

### PR-M06-08：LocalFileMessageStore Facade、Composition 与 Config

- [ ] 入口：`[ARCH]` Local 子模块已分别通过 focused test，公开 `LocalFileMessageStore`、Serde/default/alias 基线已冻结。
- [ ] `[DEV]` 保留 public type/facade，内部按 lifecycle/query/reput/cleanup 组合已迁模块；config 分为旧 Serde envelope 与 normalized backend config。
- [ ] `[DEV]` 仅做 composition 和机械拆分；任何 lifecycle/query 行为修复进入独立 PR 并先加回归测试。
- [ ] `[TEST]` focused test：public-path compile、config round-trip/default、load/start/shutdown/destroy、query/reput/cleanup。
- [ ] `[REV]` 检查约 500 行审查信号与约 800 行上限、模块单向依赖、facade 无算法回流。
- [ ] 回滚点：LocalFileMessageStore composition 指回上一组合实现，旧 config envelope 与数据目录保持。

### PR-M06-09：创建 RocksDB Foundation

- [ ] 入口：`[ARCH]` Rocks column family、key/value、snapshot和持久格式 golden 已冻结；Local 唯一 CommitLog 已稳定。
- [ ] `[DEV]` 创建 `rocketmq-store-rocksdb`，`default = []`；先机械迁移 native foundation、CQ/Index、maintenance/runtime/snapshot，不接 message-store adapter。
- [ ] `[TEST]` focused test：现有 `rocksdb_foundation_tests`、snapshot/reopen、column family和 default/local tree无 native RocksDB。
- [ ] `[REV]` 检查 foundation 只依赖 store-api/local/model/runtime/error/observability，不依赖 store facade/tiered/Broker。
- [ ] 回滚点：不启用新 Rocks crate 的 factory；Local/default 路径完全不受影响。

### PR-M06-10：RocksDB MessageStore Adapter 与 Parity

- [ ] 入口：`[TEST]` Rocks foundation和 Local semantic corpus均通过，唯一 CommitLog接口已冻结。
- [ ] `[DEV]` 迁 message-store adapter，组合 store-local CommitLog，只替换 Rocks CQ/Index 派生路径。
- [ ] `[TEST]` focused test：`rocksdb_store_semantics_tests`、Broker rocks/pop、Local/Rocks parity、restart和failure mapping。
- [ ] `[REV]` 检查不写第二消息日志、不改变 column family/offset/error语义，Client/Broker类型不泄漏。
- [ ] 回滚点：Rocks factory 指回已通过同一 parity corpus的上一 adapter；Local/default和CommitLog保持在线。

### PR-M06-11：Store Facade、Tiered 反转与 Feature 所有权

- [ ] 入口：`[ARCH]` Local/Rocks adapter均通过各自 corpus，R0 public path与feature baseline已冻结。
- [ ] `[DEV]` 现有 store 只保 backend enum/factory、legacy config/trait、Tiered decorator和精确 re-export。
- [ ] `[DEV]` tieredstore 改依赖 store-api；Local fallback/dispatch/lifecycle组合留 store facade。
- [ ] `[DEV]` feature owner调整：Local owns fast/safe/io_uring，Rocks owns native rocks，facade弱转发并保alias。
- [ ] `[TEST]` focused matrix：no-default/default/local/fast/safe/fast+safe/io_uring/rocks/tiered/observability；all-features不替代矩阵。
- [ ] `[REV]` 检查 fast+safe 仍为当前优先级，facade无实现算法，legacy compile fixture通过。
- [ ] `[HUMAN]` 批准R0 no-default可能因兼容re-export仍编译部分Local，不虚假宣称构建已变轻。
- [ ] 回滚点：feature alias、factory、Tiered decorator、legacy re-export分别回滚；不得关闭旧路径。

### PR-M06-12：依赖图与消费方收口

- [ ] 入口：`[TEST]` PR-M06-03至11的focused evidence齐全且对应同一候选快照。
- [ ] `[DEV]` 更新root workspace和dependency policy；累计package数增加3；新processor直连store-api。
- [ ] `[TEST]` 运行canonical/legacy compile、完整精确feature矩阵、Broker和受影响standalone path consumers。
- [ ] `[REV]` 证明local/rocks/tiered/facade四个closure符合目标DAG，机械迁移与行为修复提交可追溯分离。
- [ ] 回滚点：closeout失败返回具体PR修复并重新冻结，不通过扩大policy baseline收口。
- [ ] `[HUMAN]` 签署storage compatibility、唯一WAL和M06 Gate。

## 公共兼容面

- `rocketmq-store` crate、旧 MessageStore、LocalFileMessageStore、config、模块深路径和 feature alias 在 R0/R1 保留。
- 20B CQ/Index、CommitLog、RocksDB 持久布局和 Java compatibility golden 不变。
- 旧 StoreError 保兼容 adapter；新 store-api 错误不暴露 backend 实现细节。
- R0 不承诺 `--no-default-features` 已完全剥离 Local；下一 major 删除 legacy path 后再决定。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-store
cargo test -p rocketmq-broker
cargo clippy -p rocketmq-store --features rocksdb_store --all-targets -- -D warnings
cargo clippy -p rocketmq-broker --features rocksdb_store --all-targets -- -D warnings
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_foundation_tests
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_store_semantics_tests
cargo test -p rocketmq-broker --features rocksdb_store rocksdb
cargo test -p rocketmq-broker --features rocksdb_store pop_consumer
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-error-hygiene.ps1
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
git diff --check
```

### 本里程碑新增后执行

```powershell
cargo check -p rocketmq-store-api --no-default-features
cargo test -p rocketmq-store-api
cargo check -p rocketmq-store-local --no-default-features
cargo test -p rocketmq-store-local
cargo check -p rocketmq-store-rocksdb --no-default-features
cargo test -p rocketmq-store-rocksdb
cargo tree -p rocketmq-store-local -e normal
cargo tree -p rocketmq-store-rocksdb -e normal
python scripts/architecture_dependency_guard.py --mode baseline
python scripts/arc_mut_guard.py
```

精确 feature 命令按实际 manifest 落地后补入 evidence index；未定义 feature 不提前执行。

## 回滚触发器

- ack/durable watermark、dirty-tail/recovery 或持久 golden 发生未批准变化。
- store-api 泄漏 MappedFile/HA/Timer/Rocks/Tiered/Tokio/runtime 类型。
- Rocks 自建第二 CommitLog，或 local/rocks/tiered 反向依赖 store facade。
- default/local-only tree 出现 native RocksDB。
- feature alias/default/fast-safe 优先级改变，或 legacy path 无法编译。

按 API、CommitLog/recovery、flush、CQ/Index、HA/transfer、Timer/POP/services、Local composition、Rocks foundation、Rocks adapter、facade feature 的独立回滚点处理。任何已确认消息可丢失的失败立即停止里程碑，冻结证据并升级 `[HUMAN]`。

## Exit Checklist

- [ ] `[REV]` store-api 无实现泄漏，capability 足够窄。
- [ ] `[TEST]` Local dirty-tail/flush/HA/CQ/Index/recovery golden 全绿。
- [ ] `[TEST]` Rocks foundation/semantics/Broker parity 全绿，默认 tree 无 native RocksDB。
- [ ] `[REV]` CommitLog 只在 Local，Rocks/Tiered 只持派生状态。
- [ ] `[DEV]` store facade 无算法回流，旧路径/feature alias 可编译。
- [ ] `[TEST]` 精确 feature matrix 和受影响 consumer 验证完成。
- [ ] `[DEV]` dependency policy/package 数与实际一致。
- [ ] `[HUMAN]` 唯一 WAL、持久兼容与 M06 Gate 已签署。

## 交接物

- 向 M07 交付 Broker processor 可消费的 store-api 和 facade 可见性规则。
- 向 M10 交付唯一 WAL、receipt/progress、Local/Rocks golden、feature matrix 和故障 corpus。
- 向 M09 交付 store facade ledger、公共路径和下一 major 删除清单。
