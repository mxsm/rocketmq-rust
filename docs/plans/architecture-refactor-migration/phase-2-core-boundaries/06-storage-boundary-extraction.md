# M06：Store API、Local 与 RocksDB 边界提取

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 进行中；M06-01/M06-02/M06-03a/M06-03b/M06-03c/M06-03d/M06-03e/M06-03f/M06-03g/M06-03h/M06-03i/M06-03j/M06-03k/M06-03l/M06-03m/M06-03n/M06-03o/M06-03p/M06-03q/M06-03r/M06-03s/M06-03t/M06-03u/M06-03v/M06-03w/M06-03x/M06-03y/M06-03z/M06-03aa 已完成，继续 M06-03 |
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

- [x] `[ARCH]` 冻结 capability、AppendReceipt/DerivedProgress/StoreError 语义和 legacy adapter 边界。
- [ ] `[TEST]` 准备 dirty-tail、flush、HA、recovery、20B CQ/Index、Local/Rocks parity corpus。
- [x] `[DEV]` 检查 store/broker/tieredstore 目标文件和现有用户修改无重叠。
- [x] `[HUMAN]` 批准 CommitLog 唯一 WAL 与 `rocks → local → api` 单向关系。

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

- [x] `[ARCH]` 固定 StoreLifecycle、MessageAppender、MessageReader、OffsetIndex、StoreHealth、ReplicationControl、DerivedRecordSink、AdminStore。
- [x] `[DEV]` 创建 `rocketmq-store-api`，继承 workspace 元数据，`default = []`，只依赖 model/error/Bytes 类值库。
- [x] `[DEV]` 设计 backend-neutral StoreError；旧包含 Rocks/Tiered/HA 细节的错误通过 adapter 映射。
- [x] `[DEV]` 让一个真实 Broker processor 只依赖 `MessageAppender + StoreHealth`，保留旧 MessageStore adapter。
- [x] `[TEST]` 比较新旧 processor 输出、错误、writable 和 watermark 语义。
- [x] `[REV]` 检查 API 无 Tokio/runtime/remoting/observability/MappedFile/native backend 类型。
- [x] 回滚点：processor 恢复 legacy trait；新 API 可删除，不影响 store 实现。

### PR-M06-02：中立 receipt/read result 与 compatibility bridge

- [x] `[DEV]` 实现 AppendReceipt 的 first/last/appended/durable watermark 和 Durability。
- [x] `[DEV]` 实现 DerivedProgress/StoreHealth，明确派生进度不是主写 ack 条件。
- [x] `[DEV]` 为 Get/Query/SelectMappedBuffer 设计 Bytes/lease 中立结果；旧 MappedFile 结果留 Local adapter。
- [x] `[DEV]` 将旧 126 方法 trait 组合/转发到窄 capability，不新增 required method。
- [x] `[TEST]` 覆盖错误映射、lease 生命周期、receipt 等价和 legacy trait compile fixture。
- [x] `[REV]` 检查热路径用泛型/enum，只有冷边界用 `Arc<dyn Trait>`。
- [x] 回滚点：保留 API crate但撤销首个 consumer；不复制旧 trait 到新 crate。

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

## M06-01 capability spike evidence

- [x] `[ARCH]` 冻结 `StoreLifecycle`、`MessageAppender`、`MessageReader`、`OffsetIndex`、
  `StoreHealth`、`ReplicationControl`、`DerivedRecordSink` 和 `AdminStore` 八个窄能力契约。
- [x] `[DEV]` 创建 workspace crate `rocketmq-store-api`，继承 workspace 元数据，`default = []`，
  normal dependency 仅为 `rocketmq-model`、`rocketmq-error` 和 `bytes`。
- [x] `[DEV]` 用低基数 `StoreErrorKind`、稳定 operation 名和无 backend detail 的 `StoreError`
  固定 backend-neutral 错误面；native 错误仅在 legacy adapter 内分类。
- [x] `[DEV]` Broker `SendMessageProcessor` 的主单条/批量 append 与发送拒绝路径分别通过
  `MessageAppender<M>` 和 `StoreHealth` seam；现有 `MessageStore` 由借用式 compatibility adapter 保留。
- [x] `[TEST]` compile contract、全部 legacy append status 的 processor response parity、typed error、
  writable health、append/durable watermark 与 legacy adapter fixture 均有 focused 覆盖。
- [x] `[REV]` dependency policy 和 source contract 拒绝 runtime、Remoting、Observability、backend、
  Tokio、MappedFile/HA/Timer/native implementation 类型进入 API crate。
- [x] `[COMPAT]` CommitLog、CQ/Index、持久布局、`MessageStore`、`LocalFileMessageStore`、公开深路径、
  Serde/default 与 feature alias 均未移动或更改；M06-03 及后续清单保持未完成。

## M06-02 neutral receipt/read results evidence

- [x] `[DEV]` `AppendReceipt` 保留半开 first/last appended range、独立 appended/durable watermark、
  16 项中立 append status 与显式 `Memory`/`Local`/`Replicated` durability；legacy receipt 继续原样持有
  `PutMessageResult`，fallible typed constructor 拒绝 range/status/watermark/durability 矛盾，Broker
  response code/remark 未改变。outer rejected status 即使携带 inner append diagnostics 也生成 Ok rejected/no-range；
  operation-time timeout status 与 later observed durability/watermark 保持独立。
- [x] `[DEV]` `DerivedProgress` 明确拒绝充当 primary acknowledgement 或 durability condition；
  `StoreHealthSnapshot` 只使用闭合 error kind 和低基数中立健康字段。
- [x] `[DEV]` `LeasedBytes<L>`、`SelectResult<L>`、`GetResult<L>`、`QueryResult<L>` 使用 `Bytes`
  和泛型 lease；`LegacyReadLease` 私有持有并按原 `Drop` 释放 `SelectMappedBufferResult`，API crate
  不出现 MappedFile/native backend 类型。
- [x] `[DEV]` 借用式 `LegacyMessageStoreReadAdapter` 通过 closed request/result enum 转发现有
  无 filter 的 Get/Query/Select；filtered read 保留在 legacy trait。crate-private adapter-local 四方法 read port
  blanket-forward 到 `MessageStore`，compile fixture 同时组合 append/read/health capabilities，未复制或扩展
  126-method trait、未新增 public semver/coherence surface，canonical hot request 无 `Arc<dyn Trait>` 或逐次动态分配。
- [x] `[TEST]` API default/no-default tests/doctests、receipt/progress/health/read/lease tests、16 append
  status、10 get status、12 legacy error mapping、legacy compile fixture 与 M06-01 Broker 17 项 parity
  tests 全部通过；adapter 私有 behavior tests 另覆盖 size-limit/None dispatch、Get/Query/Select 全字段投影、
  lease retain/release、Bytes-before-lease drop order、`into_bytes` guard release，以及真实 mapped-file
  hold→canonical projection→release→shutdown cleanup 生命周期。
- [x] `[REV]` source/dependency contracts、architecture fixtures/baseline、ArcMut fixtures/guard、package
  和 workspace Clippy 均通过；热路径无 `Arc<dyn Trait>`、boxed future、runtime 或 blocking boundary。
- [x] `[COMPAT]` CommitLog/WAL owner、CQ/Index 与 persisted layout、公开路径、Serde/default、feature alias、
  response/watermark/buffer lifetime 均未改变；M06-03 及后续清单仍未完成。

## M06-03a store-local leaf foundation evidence

- [x] `[DEV]` 创建 workspace crate `rocketmq-store-local`，`default = []`，并由 canonical crate
  拥有 `fast-load`、`safe-load` 与 Linux-only `io_uring` feature；`rocketmq-store` 保持原 default，
  只转发同名 feature，且不再直接依赖 `tokio-uring`。
- [x] `[DEV]` 将 `direct_io`、`flush_strategy`、`mapped_buffer`、`mapped_file_error`、`metrics`
  与 `io_uring_impl` 六个无 owner 反向边的 MappedFile leaf 定义及其单元测试机械迁移到 Local；
  每个公开类型/函数只有一个 canonical 定义，facade 仅精确 re-export。
- [x] `[COMPAT]` canonical/legacy fixture 证明 Direct I/O 校验与错误文本、`FlushStrategy`、
  `MappedBuffer`/`MappedFileError`、metrics、io_uring status/capability 的类型身份与行为保持一致；
  `rocketmq_store::log_file::mapped_file` 根路径和 `io_uring_impl` 深路径继续可编译。
- [x] `[REV]` Local normal tree 只包含 `bytes`、`memmap2`、`parking_lot`、`thiserror` 及其基础闭包；
  source/manifest contract、architecture fixtures/baseline、ArcMut fixtures/guard、package/workspace
  Clippy 与 routing guard 均通过，未增加 dependency/error policy exception。
- [x] `[REVIEW]` source contract 使用 comment/string-aware active Rust 视图验证全部旧 root type/function
  re-export 与 canonical 单定义，包含 `pub fn io_uring_backend_status`；mutation fixture 拒绝注释假导出
  与第二个 active function owner。manifest mutation fixtures 拒绝 top-level/build dependency、Windows/other
  target、target build dependency 和重复的 `tokio-uring`，只接受精确 Linux target 下唯一 optional normal
  dependency。compatibility fixture 直接编译所有旧 root 路径并保留 `io_uring_impl` 深路径身份。
- [x] `[SCOPE]` 本切片未迁移 `MappedFile`/`DefaultMappedFile`、CommitLog、load/recovery、flush、
  CQ/Index、HA 或 Local composition，未改变持久格式或 runtime ownership。PR-M06-03 顶层条目与
  M06 Exit Checklist 保持未完成。

## M06-03b recovery/load neutral planning evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::{load,recovery}` is now the single canonical owner of
  `LoadStatistics`, both recovery hint enums, `RecoveryStatistics`, both abnormal-recovery range/window values,
  and the deterministic range planner with its private pure helpers.
- [x] `[COMPAT]` The existing Store loader/recovery modules exact re-export the canonical items. The loader's
  private `HintResult` orchestration remains facade-owned through private free recording helpers; the mapped-file,
  checkpoint, config, message parsing, and CommitLog owners did not move. Explicit summary-log targets preserve
  the legacy Store module targets and therefore unchanged EnvFilter/log-routing behavior.
- [x] `[TEST]` RED/GREEN identity fixtures and the mutation-resistant source contract prove one definition per
  item, unchanged legacy module visibility/path behavior, exact facade re-exports, and seven fixed recovery-window
  outcomes. Local unit tests, CommitLog recovery integration, the exact feature matrix, package/workspace Clippy,
  architecture dependency gates, ArcMut gates, and the AGENTS routing guard pass.
- [x] `[SCOPE]` This slice does not move mmap/prefetch execution, `DefaultMappedFile`, flush/group commit, CQ/Index,
  HA, runtime ownership, or persisted formats. The PR-M06-03 checklist and the M06 Exit Checklist remain open.

## M06-03c mapped-file progress and lifecycle kernel evidence

- [x] `[DEV]` `rocketmq-store-local::mapped_file::kernel` is the single canonical owner of
  `MappedFileProgress`, `ReferenceResource`, `ReferenceResourceBase`, and `ReferenceResourceCounter`.
  `MappedFileProgress` owns the legacy file size, wrote/committed/flushed positions, store/last-flush timestamps,
  and start/stop timestamps with the original initial values and atomic orderings.
- [x] `[COMPAT]` `DefaultMappedFile` now composes the Local progress/lifecycle kernel and delegates every legacy
  `MappedFile` progress method. The old Store `reference_resource` and `reference_resource_counter` modules are
  exact narrow re-exports, so their existing crate visibility and type identity are unchanged. Direct and
  transient-store readable positions, segment-full equality, successful-flush-only durable advancement, append,
  commit, warmup timestamp, graceful/forced shutdown, and cleanup-once behavior remain unchanged.
- [x] `[TEST]` RED first proved the Local kernel and owner contract were absent. GREEN covers six deterministic
  Local progress/lifecycle cases, 26 existing `DefaultMappedFile` cases including an injected flush failure,
  the active-Rust ownership contract, the exact Local feature matrix, recovery compatibility, package/workspace
  Clippy, architecture dependency gates, ArcMut gates, and AGENTS routing.
- [x] `[REV]` The 26-case mutation-resistant contract strips comments and strings and applies an explicit,
  conservative Store policy instead of claiming complete recursive Rust use-tree parsing: any active `use` or
  `extern crate` rooted at `rocketmq_store_local` with an alias/brace tree is forbidden; every public Store glob
  re-export is forbidden; and public uses containing the four protected items must match the three canonical
  lifecycle statements exactly. Owner checks include private/`pub`/`pub(crate)` struct, trait, type, enum, union,
  module, type-alias, and use-as occurrences. The contract also rejects every old progress field name regardless
  of type/access, requires exactly one `MappedFileProgress` field named `progress`, and preserves the Local
  dependency closure. Thirteen negative fixtures cover all three review rounds. The sole ArcMut import fingerprint
  relocation remains one-for-one and the governed occurrence count remains 3377.
- [x] `[SCOPE]` This slice does not move `File`, mmap/`ArcMut`, `DefaultMappedFile`, messages/callbacks, config,
  `TransientStorePool`, flush owner, CQ/Index, HA, runtime ownership, or persisted formats. PR-M06-03 and every
  M06 Exit Checklist item remain open.

## M06-03d mapped-file storage handle kernel evidence

- [x] `[DEV]` `rocketmq-store-local::mapped_file::file::MappedFileStorage` is the single canonical owner of the
  operating-system `File`, authoritative `PathBuf`, and parsed segment offset. It mechanically owns the legacy
  metadata snapshot, non-truncating read/write/create open, `set_len`, grow-only preallocation decision,
  rename/path update, read-only reopen, and delete operations. File size remains owned only by
  `MappedFileProgress`; storage does not duplicate it.
- [x] `[COMPAT]` Store `platform` exact re-exports the canonical preallocation constant, outcome type,
  classifier, and function. `DefaultMappedFile` composes exactly one Local storage handle while retaining
  directory creation/path validation, the `CheetahString` projection, every tracing/observability target and
  message, mmap/`ArcMut`, messages, config, progress, and flush ownership. Failed rename leaves path/projection/
  handle unchanged; successful rename updates Local path and the Store projection before legacy read-only reopen;
  reopen failure preserves the renamed path; delete success/failure logging remains Store-owned.
- [x] `[TEST]` RED first proved the Local file owner and Store identity path were absent. GREEN covers numeric and
  invalid segment names, prefix preservation, grow/shrink/zero-length and deterministic preallocation decisions,
  outcome classification, canonical path/offset, rename/reopen/delete ordering, eager/lazy mmap, transient
  commit/read, injected flush failure, Store type identity, recovery compatibility, and the exact Local feature
  matrix.
- [x] `[REV]` The 41-case mutation-resistant contract requires exactly one canonical file owner with only
  `File`/`PathBuf`/offset fields, exact Store platform re-exports and parse wrappers, exactly one
  `storage: MappedFileStorage`, no field resolving through fully-qualified paths or direct non-aliased imports to
  `std::fs::File`, `std::path::PathBuf`, or plain `u64`, one Unix-only normal `libc` dependency, and the unchanged
  forbidden dependency closure. Every active alias/brace use and type alias in DefaultMappedFile is governed by
  an exact syntax allowlist; the contract does not claim to parse arbitrary Rust use trees or generic aliases.
  Negative fixtures reject comments/strings, non-allowlisted direct/brace aliases, generic/default type aliases,
  duplicate owners/fields, renamed storage fields, aliased field types, and misplaced dependencies while proving
  `AtomicU64` and the complete existing allowlist are accepted. Both mmap calls document completed `set_len`,
  owner no-truncation, `ArcMut` mapping lifetime independent of file-handle rename/reopen/close, and the lazy
  initialization lock. The legacy `get_file` Rustdoc explicitly records the caller-enforced no-size-change
  compatibility contract; every Local file public API documents errors, panics, and lifecycle ordering where
  applicable.
  Architecture/routing gates pass; five governed DefaultMappedFile/ArcMut fingerprints use exactly five direct
  one-for-one approvals with no new occurrence, so the governed count remains 3377.
- [x] `[SCOPE]` This slice does not move mmap, `ArcMut`, `DefaultMappedFile`, `MappedFile`, messages/callbacks,
  config, warmup/memory locking, `TransientStorePool`, flush/group commit, CQ/Index, HA, CommitLog orchestration,
  runtime ownership, or persisted formats. PR-M06-03 and every M06 Exit Checklist item remain open.

## M06-03e generic mmap lifecycle kernel evidence

- [x] `[DEV]` `rocketmq-store-local::mapped_file::mapping` is the single canonical owner of `LazyMmapStats` and
  generic `MappedFileMapping<M>`. The kernel owns one `OnceLock<M>`, one initialization mutex, lazy enablement,
  and the four operation/failure/total/last-latency atomics. Eager and lazy construction, double-checked fallible
  initialization, failure retry, initialized identity, and statistics snapshots are now Local behavior.
- [x] `[COMPAT]` `DefaultMappedFile` composes exactly one `MappedFileMapping<ArcMut<MmapMut>>` and delegates its
  existing lazy-enable, mapped-state, statistics, and mapping access paths. Store retains both unsafe
  `MmapMut::map_mut` calls, `ArcMut`, the open file handle, all reference-returning getter signatures, safety
  contracts, logging/metrics/observability, and actual mmap creation. The legacy `LazyMmapStats` path is an exact
  re-export of the Local type.
- [x] `[TEST]` RED first failed because the Local mapping module and both canonical owners were absent. GREEN
  deterministically covers eager zero statistics, lazy eligibility, success counters/latency, failed retry,
  eight concurrent callers with exactly one initializer, value identity, and monotonic statistics. Full Local,
  DefaultMappedFile, storage/CommitLog compatibility, load/recovery, and the exact Local feature matrix pass.
- [x] `[REV]` The 47-case mutation-resistant contract proves one Local owner per lifecycle type, the exact Local
  state fields, one exact Store generic composition, no direct Store mmap cell/init lock/lazy flag/stat counters,
  no Local `ArcMut` or forbidden dependency, exact re-export, and unchanged getter signatures. Negative fixtures
  reject renamed fields, type/import aliases, duplicate generic owners, alias/brace/glob facades, comments, and
  strings. Architecture gates pass. Six governed fingerprints use direct one-for-one BASE-to-HEAD approvals;
  ArcMut occurrences remain 3,377.
- [x] `[SCOPE]` This slice does not replace `ArcMut`, enable `arc_lock`, add an `RwLock` or second mapping, change
  zero-copy/reference getter behavior, or move mmap creation, mapped buffers/results, append/direct-write/flush/
  warmup/memory-lock algorithms, `DefaultMappedFile`, config, CommitLog orchestration, CQ/Index, or HA. PR-M06-03
  and every M06 Exit Checklist item remain open.

## M06-03f CommitLog frame cursor and magic ownership evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::record` is the canonical storage-boundary owner of the CommitLog
  V1 message magic, blank magic, pure blank-marker check, static `CommitLogFrameSource`, and generic
  `CommitLogFrameCursor<S>`. The cursor retains the 64-KiB batching order, direct oversized-frame reads,
  cross-batch refill, absolute offsets, and buffer reset while stopping without advancement at incomplete headers,
  non-positive sizes, unavailable reads, and declared dirty-tail overruns.
- [x] `[COMPAT]` Store CommitLog constants and the recovery blank helper are exact Local re-exports.
  `BatchMessageIterator<'a>` keeps its exact constructor/method signatures and contains only a Local cursor over a
  private static `MappedFile` adapter; no iterator algorithm remains in Store. Common and Protocol retain their
  pre-existing wire-codec constants as separate compatibility surfaces; this slice does not claim a whole-repo
  constant owner and does not move V2, CRC, properties, dispatch, configuration, mmap, or persisted bytes.
- [x] `[TEST]` RED/GREEN Local goldens cover empty/short sources, one/multiple frames, non-positive sizes,
  dirty tails, exact/oversized/crossing 64-KiB frames, blank recognition, and final offsets. Real
  `DefaultMappedFile` tests prove oversized, cross-batch, dirty-tail, constant-path, helper-path, function-identity,
  and legacy-signature compatibility. The mutation-resistant contract proves the Local+Store owner boundary,
  exact re-exports, wrapper-only Store iterator, static source port, and forbidden dependency closure while
  rejecting duplicate owners, copied algorithms, aliases/brace/glob imports, comments, strings, and `dyn` ports.
- [x] `[REVIEW]` The final contract extracts and normalizes the exact `impl<'a> BatchMessageIterator<'a>` block,
  requires its three legacy signatures and pure delegation bodies, and rejects a wrong constructor lifetime,
  copied peek/size/refill parsing, a hard-coded offset, fully-qualified or aliased dynamic ports, and every active
  alias/brace import in the Local record and Store recovery boundary files. Scripted sources deterministically
  prove initial `None`, parseable-short refill, and oversized direct-short reads stop at offset zero with one/two
  bounded calls. A private pure fit helper freezes equal-end acceptance and `usize::MAX` overflow rejection.
- [x] `[REV]` Local/Store/workspace Clippy, Local `-D warnings` Rustdoc, exact Local feature checks, architecture
  dependency gates, and ArcMut gates pass. ArcMut investigation found three direct production fingerprint
  relocations caused by the wrapper extraction; the baseline changes only those three one-for-one occurrence
  objects, adds no test approval, and remains 1,232 identities/3,377 occurrences.
- [x] `[SCOPE]` No message/property/CRC/V2 parsing, `DispatchRequest`, recovery context/config/checkpoint,
  mmap/`ArcMut` representation, `DefaultMappedFile` ownership, append/flush/group commit, CQ/Index, or HA moved.
  PR-M06-03 and every M06 Exit Checklist item remain open.

## M06-03g bounded CommitLog record parser evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::record_parser` 已成为 Local+Store 存储边界内 V1/V2 有界
  record parser、中立 DTO、结构错误与静态 checksum port 的 owner；Store 删除 parser 模块副本，仅保留
  checksum/config、property/dup/tag/delay、property CRC、inner-batch、`DispatchRequest` 映射和最终事务推进。
- [x] `[COMPAT]` 提交 A 固化 fail-closed 与事务推进规则，提交 B 仅机械搬移 parser。去除 Rustdoc、路径和公开
  可见性差异后，A/B 从 `RecordReader` 到测试模块前的算法文本完全相同；同一 fail-closed corpus 在 A/B
  均为 10/10。Common、Protocol 与 Local V1/V2 magic 仅比较值兼容，不声明全仓唯一 owner。
- [x] `[TEST]` 最终 focused evidence：fail-closed/transaction/whole-value 13/13、Local 全量 85 项、Store
  property-CRC/inner-batch 2/2、record compatibility 3/3、Local ownership/mutation contract 62/62。合法 V1
  whole-value golden 覆盖双 IPv6 host、topic、queue/physical offset、sysflag、store timestamp、tags、keys、
  uniq、dup 与 inner batch；两份 V2 golden 覆盖 delay clamp+table 和缺表 fallback。
- [x] `[REVIEW]` Blank 在返回前验证 `declared >= 8 && declared <= available`，合法 blank 仍仅推进 8 字节；
  `CommitLogRecord` 保持 inline，enum 上最窄 Clippy reason 明确热路径禁止每消息 heap allocation。contract
  对 comment/string-aware active Rust 保守拒绝任意 `Box`、fully-qualified `std::boxed::Box` 与 parser type
  alias，并杀死 blank boundary 删除。Store wrapper contract 锁定精确旧签名、恰好一次 Local decode、三处
  精确事务 advance，拒绝 `from_be_bytes`、input Buf get/copy/slice/index parsing；继续约束只读输入、无
  Store copy、无 `dyn`/新增依赖、alias/brace/glob import 或 duplicate owner。
- [x] `[REV]` 五组 Local feature check、Local/Store/workspace Clippy、Local `-D warnings` Rustdoc、架构
  35 项+fixtures+baseline、ArcMut 63 项+fixtures+final guard、格式与 diff 检查通过。
- [x] `[SCOPE]` M06-03g 已完成；PR-M06-03 父项和 M06 Exit Checklist 保持未完成。本切片不迁移
  append/flush/group commit、CQ/Index、HA、Timer/POP 或 Store composition。

## M06-03h normal recovery dual-watermark state machine evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::recovery` 现拥有 runtime-neutral `NormalRecoveryState`、
  `NormalRecoveryPolicy`、五种 event、三种 action、typed offset error 与双水位 summary。state 严格只含
  `last_valid_offset`、`truncate_offset` 和 policy；全部 base+relative、start+size 使用 checked arithmetic，
  两个候选水位在提交前验证不超过 `i64::MAX`，任何错误保持 state 不变。
- [x] `[COMPAT]` Standard 保留 frame-start last-valid、frame-end truncate、invalid/source-end 全局停止和
  blank roll；Optimized 保留 frame-end 双水位及 blank/invalid/source-end 继续下一 segment。Store 仍拥有
  MappedFile/reader、record parser/CRC/DispatchRequest、日志、normal zero-dispatch、outer segment switch、
  controller clamp、CQ truncate 与 flushed/committed/truncate 写回。复审修复以 checked `TryFrom` 恢复 CQ
  `max < 0` 时触发 truncate 的旧语义；四个 async public 签名保持不变。
- [x] `[TEST]` Local 9 项覆盖 fallible constructor 的 `i64::MAX`/`MAX+1` 边界、2×4 event 矩阵、连续消息、
  first empty、blank→next empty、invalid first+
  valid second、initial confirm、last blank、三种 overflow、`i64` 上界和 error transaction。真实 Store
  goldens 覆盖 blank-first+valid-second、SourceEnded/invalid-first+valid-second、后续 empty segment、双水位
  和 normal zero-dispatch，并验证负 CQ 参数不改变两条 public normal 路径的其余水位；recovery 13/13、
  load 7/7（1 ignored）、record 13/13、compatibility 3/3+2/2。
- [x] `[REVIEW]` 67 项 comment/string-aware contract 证明六个 Local type 的唯一 owner、fallible constructor、
  精确 state fields、
  start/end policy、checked arithmetic、无 Store orchestration/ArcMut/dyn/alias/brace/glob；锁定四个 public
  签名、两条 normal 各五个 reducer event/action、MessageAccepted 三 action、current_pos 全局 stop、唯一
  summary 绑定及最终写回数据流、无 Store watermark/policy copy，并杀死 empty action、plain add、constant
  summary、branch bypass、start/end 交换、SourceEnded 虚构 kind、Store policy copy 和 reducer 删除。
- [x] `[REV]` 两条 abnormal 函数 normalized hash 从 BASE 到 HEAD 分别保持 `c16a626f…` 与 `272dd669…`；
  Local 全量 94 项、五组 feature check、Local/Store/workspace Clippy、Local `-D warnings` Rustdoc、架构
  35 项+fixtures+baseline、ArcMut 63 项+fixtures+final guard、格式与 diff 检查通过，且无新增依赖。
- [x] `[SCOPE]` M06-03h 只接入两条 normal recovery；abnormal recovery、dispatch、持久格式、flush/runtime、
  checkpoint/window selection、controller/dup 条件及 CQ/Index/HA 均未迁移。PR-M06-03 父项和 M06 Exit
  Checklist 保持未完成。

## M06-03i abnormal recovery triple-watermark state machine evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::recovery` 现拥有 runtime-neutral
  `AbnormalRecoveryState`、standard/optimized policy、ungated/confirm-bounded dispatch gate、五种 event、
  六种 action、typed offset error 与 `last_valid_offset`/`confirm_valid_offset`/`truncate_offset` 三水位
  summary。state 只含三水位与 policy；base+relative、start+validated-size、encoded physical offset+input-size
  全部 checked，负 offset、溢出或超过 `i64::MAX` 时 fail closed 且 state 保持不变。
- [x] `[COMPAT]` Store 两条 abnormal recovery 只把纯 offset/action 决策交给 Local；MappedFile/window、I/O、
  parser/CRC、dup/controller bounded 路径对每条有效消息读取的 fresh confirm limit、dispatch/stats、blank
  file-end hook 与 CQ truncate 仍由 Store 拥有；ungated 路径不读取 confirm limit。最终
  flushed/committed/truncate 写回仍由 Store 拥有。Standard 保留 accumulator/start last-valid，Optimized
  保留 absolute frame-end last-valid；controller 最终使用 confirm-valid clamp，非 controller 使用 last-valid。
  Normal recovery API/实现、checkpoint/window 选择、持久字节、公开签名与依赖均未改变。
- [x] `[TEST]` Local 7 项覆盖 constructor/i64 边界、segment start、policy×gate、encoded candidate 与
  validated size 分离、fresh gate limit、blank/invalid/source matrix、overflow/negative 与错误事务性。真实
  Store 19 项 recovery integration 包含 dirty tail、blank hook、invalid/source、后续 empty segment、负 CQ，
  并对 Standard/Optimized 分别证明 dup 的 first eligible/second skipped 仍截断两帧，以及 controller 的
  first eligible/second ineligible 最终 clamp 到第一条 encoded input end；Store lib 573 项通过。
- [x] `[REVIEW]` 72 项 comment/string-aware contract 锁定 Local 唯一 owner、精确 state fields、checked
  arithmetic、policy/action/gate 矩阵，以及 Store 两条 adapter 的 input-size candidate、validated-size
  advance、bounded fresh gate、skip-without-dispatch、blank hook、stats、唯一 summary、controller/non-controller
  与 CQ/physical truncate 数据流；contract 还锁定 `process_message` 位于 candidate/gate/apply 之前，并要求
  optimized 的 `file_processed` 在完整 action match 成功后统一更新，使 valid-but-skipped 记录仍计入文件统计。
  24 个 reviewer mutation 覆盖边界比较、gate bypass、constant summary、错误 candidate、late process、
  dispatch-only stats、CQ/controller 分支和 Store policy copy。
- [x] `[REV]` Local 全量 101 项、五组 feature check、Store recovery 19 项、Store lib 573 项、Local/Store/
  workspace Clippy、Local `-D warnings` Rustdoc、架构 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+
  final guard、格式与 diff 检查通过；真实 gate golden 没有新增 `ArcMut`/`LocalFileMessageStore` occurrence。
- [x] `[SCOPE]` M06-03i 只接入两条 abnormal recovery。PR-M06-03 父项和 M06 Exit Checklist 保持未完成；
  append/flush/group commit、CQ/Index、HA、Timer/POP、Local composition、runtime ownership 与持久格式均未迁移。

## M06-03j CommitLog file metadata and validation evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::load` 现唯一拥有
  `CommitLogFileMetadata`、`CommitLogFileLoadDecision`、`CommitLogFileValidationError` 与纯
  `validate_commit_log_file`。Local 只按 path/actual/expected/expected-size/is-last 决定 Load、删除空尾文件或
  typed mismatch error，不拥有 filesystem metadata/delete、rayon、CheetahString、DefaultMappedFile 或 mmap。
  `expected = 0` 合同已冻结：last empty 删除、non-last empty 加载、non-empty 拒绝。
- [x] `[COMPAT]` Store 删除私有 `FileMetadata` 及未使用的 `file_name` 字段，并让 canonical metadata 从
  `fs::metadata` 贯穿到 mapped-file factory。parallel rayon map 与 sequential for 各在原位置调用一次 Local
  validator；Load 保留，RemoveEmptyLast 继续原地 non-fatal delete+warn 并过滤，错误继续精确映射为
  `InvalidData`。read-dir/filter/sort/>4、indexed parallel collect/flatten、sequential first-error、lazy/eager mmap、
  memory hints、position/statistics/time 和公开 loader API 均未改变；legacy `files_removed` 仍为 0。
- [x] `[TEST]` Local 7 项覆盖 exact Load、empty-last remove、non-last empty、short、long、expected-zero matrix，
  以及 error fields/`Error`/逐字 Display。Store loader 14 项和 load integration 7 项（另 1 项 stress ignored）覆盖
  sequential first-error 不删除后续空尾、parallel combined corruption、空尾过滤、顺序、统计、lazy last 与精确
  error；空尾过滤 fixture 显式启用 lazy mmap，并证明过滤后前三个 historical 文件保持 lazy/unmapped、最后一个有效
  文件保持 eager/mapped。recovery integration 19 项及 Store lib 576 项通过。
- [x] `[REVIEW]` 75 项 comment/string-aware contract 锁定四个 Local owner、公开字段/枚举/函数签名与纯 decision
  matrix，要求三个新增类型在 Store 仅 private import。两条 collector 的 validator 必须位于 rayon map/for 内且
  位于真实 metadata 读取之后，mmap 创建必须等待完整 validation；同时锁定 non-fatal delete+warn、InvalidData、
  indexed order/flatten、sequential first-error、公开 loader 签名、canonical metadata flow 与 `files_removed = 0`。
  contract 分别提取 parallel/sequential 的 RemoveEmptyLast arm 和内层 remove-file Err body，要求失败分支只能 warn，
  禁止 return-Err/Err/panic/unwrap/expect/?；parallel 必须以 `Ok(None)` 结束，sequential 必须正常过滤后继续。
  Local 5 个及 Store 17 个 mutation 覆盖 wrong-last、bypass、复制校验、decision swap、两条有效 Rust fatal-delete、
  错误 kind/text、validation 移出 closure、order/flatten、alias/brace/glob、签名和统计篡改。
- [x] `[REV]` Local 全量 108 项、五组 feature check、Local/Store/workspace Clippy、Local `-D warnings` Rustdoc、
  Store 普通 Rustdoc、架构 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、格式与 diff 检查通过。
  canonical metadata 参数只改变既有 `DefaultMappedFile` return-type 邻接 fingerprint；BASE→HEAD relocation 已按
  ADR-013 精确批准，promoted baseline 仍为 1,232 identities/3,377 occurrences，没有新增或移动 governed occurrence。
- [x] `[SCOPE]` M06-03j 只迁移 CommitLog 文件长度验证边界；filesystem orchestration、mmap、append/parser/recovery、
  flush/group commit、CQ/Index、HA、Timer/POP、runtime ownership 与持久格式均未迁移。PR-M06-03 父项和 M06 Exit
  Checklist 保持未完成。

## M06-03k CommitLog mapping plan and hint statistics evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::load` 现拥有纯 `CommitLogMappingPlan`：它按已校验、已过滤的
  metadata 顺序一次性决定 sequential/parallel 执行方式及 eager/lazy-read-only 映射模式。parallel 仅在选项启用且
  过滤后文件数大于 4 时选择；lazy 仅用于选项启用时的非末尾文件。Store 不再按 index/count 重算这些决策。
  同一 Local 模块也唯一拥有 `HintOutcome` 以及 mmap advice/file prefetch 两个饱和统计归约器。
- [x] `[COMPAT]` Store 仍拥有目录与 filesystem metadata、rayon 调度、`DefaultMappedFile`/真实 mmap、平台 hint
  系统调用、日志和最终 `LoadStatistics`。parallel hint outcome 先按映射顺序收集，再顺序归约到 canonical stats；
  sequential 路径原位归约。disabled/unsupported/Windows `Ok(false)`、lazy-unmapped skip 均视为 not-attempted。
  既有 `LoadStatistics`、两个 hint 函数 re-export、`CommitLogLoader` 构造/`with`/`load` 签名和模块可见性均未改变。
- [x] `[TEST]` A/B 均先以缺失 Local owner 的编译失败建立 RED。最终 Local focused 23 项、Local 全量 124 项、
  Store loader 15 项、load integration 7 项（另 1 项 ignored）、recovery integration 19 项和 Store lib 577 项通过。
  hint goldens 覆盖 not-attempted、成功/失败隔离、sub-ms、`Duration::MAX` clamp 与所有计数/耗时饱和；映射 goldens
  覆盖顺序、过滤后阈值、末尾 eager、历史 lazy，以及 Store raw 5/raw 6 行为兼容。
- [x] `[REVIEW]` 81 项 mutation-resistant contract 锁定 Local plan/hint owner、精确字段与构造器、by-value outcome、
  饱和归约、Store private import、一次性 plan、两条 collector 的有序归约和公开 API 兼容，并拒绝复制 owner、阈值/
  mode/统计族篡改、别名与公开 re-export。架构 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard 通过。
- [x] `[REV]` 五组 Local feature check、Local/Store/workspace Clippy、Local `-D warnings` Rustdoc、Store 普通
  Rustdoc、格式与 diff 检查通过。8 个既有 governed occurrence 因 loader 参数/返回值及 hint outcome 边界相邻文本
  变化发生直接一对一 relocation，均按 ADR-013 精确批准；promoted baseline 保持 1,232 identities/3,377 occurrences。
- [x] `[SCOPE]` M06-03k 未迁移 filesystem 校验/删除、真实 mmap/平台系统调用、append/parser/recovery、flush/group
  commit、CQ/Index、HA、Timer/POP、runtime ownership 或持久格式。PR-M06-03 父项和 M06 Exit Checklist 保持未完成。

## M06-03l CommitLog recovery hint platform execution evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::load` 现唯一拥有公开安全的
  `apply_recovery_mmap_advice` 与 `apply_recovery_file_prefetch`。两者只接受 Local enum、`&MmapMut` 和文件名并返回
  `HintOutcome`，不返回 `Result`，不暴露裸指针/unsafe API、generic trait、`DefaultMappedFile` 或 `ArcMut`。
  disabled/unsupported 在计时前返回；真实平台失败只记录 warning 和 failure outcome，不传播到 loader。
- [x] `[PLATFORM]` Unix mmap advice 使用 `memmap2::Advice::Sequential`；Windows prefetch 由 Local 私有 helper
  直接调用 `PrefetchVirtualMemory`。Windows 依赖固定为 `windows 0.62.2`，且仅启用 `Win32_System_Memory` 与
  `Win32_System_Threading`；没有引入 `rocketmq-error`。unsafe 调用具有紧邻 `SAFETY` 注释，Local warning 使用既有
  `rocketmq_store::log_file::commit_log_loader` target、原 warning 文本和兼容的手工 storage-read 错误文本。
- [x] `[COMPAT]` Store `apply_memory_hints` 先跳过 lazy-unmapped 文件，再取得一次 mmap/file name 并调用两个 Local
  adapter；loader 中的 Unix/Windows 实现及 memmap/platform helper import 已删除。Store
  `utils::ffi::prefetch_virtual_memory` 的签名与函数体零改动。Store 继续拥有 filesystem、rayon、mapped-file factory、
  position、append/recovery/flush，以及公开 `LoadStatistics`/hint enum re-export 和全部 loader API。
- [x] `[TEST]` focused RED 分别因两个 Local API 缺失和 canonical owner 缺失而失败；GREEN 后 Windows 与 WSL/Linux
  Local focused 均为 25/25，Store loader 均为 15/15。Local 全量 127 项、Store load 7 项（另 1 项 ignored）、
  recovery 19 项、Store lib 577 项通过。纯 mapper 确定性覆盖 `Ok(true)`/`Ok(false)`/error；Store 启用两个 hint 后
  证明 historical lazy 文件保持 unmapped，且统计只包含平台支持的 eager final-file attempt。
- [x] `[REVIEW]` 82 项 contract 及 34 个本切片 reviewer mutation 锁定 Local 单一平台 owner、精确安全 API、
  false/error mapping、非传播失败、计时边界、日志、Store skip 顺序、私有精确 import、无公开 re-export、无重复/
  alias/brace/glob owner、无直接 Store OS 调用，以及兼容 ffi 完整函数。Windows/WSL check 与 Local/Store Clippy、
  Windows workspace Clippy、Local strict Rustdoc、Store 普通 Rustdoc、routing 和架构门禁均通过。
- [x] `[ARC]` BASE 8596→candidate 初扫为 1 NEW/3 STALE：Store 两个平台函数删除产生两个真实
  `DefaultMappedFile` governed occurrence 删除，不建立 relocation；唯一仍存在的 module import 以
  `ded036bc867beefc6db3270a`→`710eb3f2dd4a57100c5e0fe1` 一对一 ADR-013 approval 处理。promoted baseline 为
  1,232 identities/3,375 occurrences，final guard 通过且没有新增债务。
- [x] `[SCOPE]` M06-03l 不迁移 filesystem、rayon mapping/factory、position、append/parser/recovery、flush/group
  commit、CQ/Index、HA、Timer/POP、runtime ownership 或持久格式。PR-M06-03 父项、M06 Exit Checklist 与
  M06-04..12 保持未完成。

## M06-03m CommitLog filesystem metadata collection evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::load` 现唯一拥有
  `CommitLogMetadataCollectionOptions` 与 `collect_commit_log_metadata`。公开入口按 raw path 数量决定执行方式：
  仅在 parallel 启用且 raw count 大于 4 时进入 indexed rayon collector；raw last index 在过滤前计算。parallel
  结果按输入顺序 flatten，sequential 以 `fs::metadata(path)?` 保持 first-error；两路均将长度 mismatch 精确映射为
  `InvalidData`。空尾删除为私有 best-effort helper，失败只使用既有 target 和逐字 warning，不传播错误。
- [x] `[COMPAT]` Store loader 仅私有导入 Local collector/options，并删除两条旧 metadata collector、validator/decision/
  metadata import。Store 仍拥有目录 read/filter/sort、metadata timing 与 totals、`files_removed = 0` 兼容统计、映射计划、
  rayon mapped-file factory、memory hints、position 更新和公开 loader API。mapping threshold 继续依据过滤后 metadata，
  filesystem collection threshold 继续依据 raw paths；feature 定义、持久格式和错误文本均未改变。
- [x] `[TEST]` RED 先证明 Local API/owner 缺失；GREEN 后 Windows 与 WSL/Linux Local focused 均为 36/36，Store
  loader 均为 15/15。Windows Local 全量 139 项、Store load 7 项（另 1 项 ignored）、recovery 19 项、Store lib
  577 项通过。新增 goldens 覆盖 0/1/4/5 paths、parallel disabled、raw-five 空尾过滤、ordered parallel、sequential
  first-error 不删除后续空尾、单 missing-path parallel error context、InvalidData 与 delete-failure non-fatal。
- [x] `[REVIEW]` 82 项 contract 锁定两个新增 Local owner、options 字段/derive/可见性、公开 API、四个私有 helper、
  raw threshold/last-index、indexed order/flatten、sequential `?`、shared validation/filter adapter、原 error context、
  warning target/text 和非致命删除；Store 只允许两个精确 private import 与一次 adapter call，且禁止保留旧 collector、
  `fs::metadata`/remove/validator owner。Local 15 个及 Store 9 个 focused mutation 均被拒绝。
- [x] `[PLATFORM]` Local default/no-default/fast/safe/fast+safe/all 六组 check 通过；Store default/all，以及显式
  `local_file_store` owner 下的 fast、safe、fast+safe 组合通过。Store raw no-default、raw fast/safe/fast+safe 仍因既有
  空 `GenericMessageStore` 产生 124 个 E0004 和 unused `ArcMut`，未标记为通过；BASE→HEAD 对
  `message_store.rs`、Store/root manifest 的 diff 为零，证明本切片未引入该基线问题。
- [x] `[REV]` Local/Store/workspace all-target/all-feature Clippy、Local strict Rustdoc、Store 普通 Rustdoc、routing、
  架构 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、格式和 diff 检查通过。WSL 隔离 target 的
  focused/default 编译通过后已清理 8,386 files/4.2 GiB。ArcMut BASE→candidate 保持 1,232 identities/3,375
  occurrences，0 NEW/0 STALE；仅 9 个既有 occurrence 的 line 元数据变化，id/fingerprint/item 全部不变，因此没有
  relocation approval，canonical baseline 保持零 diff。
- [x] `[SCOPE]` M06-03m 只迁移 CommitLog filesystem metadata/validation/empty-last filtering；目录枚举、mapped-file
  factory、position、append/parser/recovery、flush/group commit、CQ/Index、HA、Timer/POP、runtime ownership 与持久
  格式仍未迁移。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03n CommitLog file discovery evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::load` 现唯一拥有 `CommitLogFileDiscovery` 与
  `discover_commit_log_files`，负责目录缺失判定、根目录枚举、条目错误跳过、regular-file 过滤及按文件名 UTF-8
  `Option` 比较的稳定排序；Store 仅通过两个私有 import 做 exhaustive adapter match。
- [x] `[COMPAT]` `DirectoryMissing` 保留原 warning 并维持 `total_load_time_ms == 0`；`NoFiles` 保留原 info
  并记录 elapsed；根 `read_dir` 错误继续传播，单条 entry 错误继续跳过，子目录继续排除，`"10" < "2"`、
  non-UTF `None < Some` 排序语义均冻结。没有 Store public re-export、公开签名、持久格式或 feature 变化。
- [x] `[TEST]` TDD RED 分别由 Local 缺少 discovery owner 的 E0432 与 contract owner 缺失证明；GREEN 后
  Windows Local focused 41/41、Local 全量 144 项（9 doctests ignored）、Store loader 16/16、load integration
  7 passed/1 ignored、recovery integration 19/19、Store lib 578/578；M06 contract 84/84，新增 10 个 Local 与
  9 个 Store mutation 审查覆盖。
- [x] `[REVIEW]` Local default/no-default/fast/safe/fast+safe/all，Store default/all 及真实
  `local_file_store` owner 的 fast/safe/fast+safe 组合均通过。四个 raw Store no-default 组合仍各以 124 个既有
  E0004 退出 101；BASE `a9b26f62bada1112d0875d4474672f6284b843a5` 对三个 owner/feature 文件为零 diff。
- [x] `[PLATFORM]` WSL/Linux isolated target 实际通过 Local focused 42/42（含 non-UTF filename fixture）与
  Store loader 16/16；`cargo clean --target-dir target/wsl-m06-03n` 已删除 5,920 files/3.4 GiB，Windows 根
  `read_dir` error、缺失/空目录 timing 与稳定字符串排序也有独立覆盖。
- [x] `[REV]` Local/Store/package 与 workspace Clippy、strict Local Rustdoc、Store Rustdoc、格式/diff、
  AGENTS routing、架构 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+initial/final guard 全部通过。
  BASE→candidate 直审均为 1,232 identities/3,375 occurrences、0 NEW/0 STALE/0 semantic changes；仅 9 个
  既有 occurrence 的 line 元数据变化，baseline blob 与 BASE 相同且无需 relocation approval。
- [x] `[SCOPE]` M06-03n 只迁移 CommitLog 目录发现、过滤与排序；metadata validation/empty-last 语义、
  mapped-file factory、position、append/parser/recovery、flush/group commit、CQ/Index、HA、Timer/POP、runtime
  ownership 与持久格式均未改变。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03o CommitLog append values and minimal mapped-file config evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::append` 现唯一拥有 `AppendMessageStatus`、
  `AppendMessageResult`、`PutMessageContext`、`CompactionAppendMsgCallback`，
  `rocketmq-store-local::config` 现唯一拥有 `FlushDiskType`。Local 只新增普通 `serde` 与测试专用
  `serde_json` 依赖，`bytes` 继续复用既有依赖。
- [x] `[COMPAT]` Store 的 `base::message_status_enum::AppendMessageStatus`、
  `base::message_result::AppendMessageResult`、`base::put_message_context::PutMessageContext`、
  `base::compaction_append_msg_callback::CompactionAppendMsgCallback` 与
  `config::flush_disk_type::FlushDiskType` 五个旧深路径均为 direct exact `pub use`，与 Local canonical owner
  是同一 Rust 类型/trait。逐字段、derive、default、Display、supplier precedence、context slice API、trait
  签名及 `FlushDiskType` 四个 Deserialize 词汇与 unknown-variant 行为均保持不变。
- [x] `[TEST]` TDD RED 由 Local 缺少 append/config owner 的 5 个 E0432、缺少测试依赖及 owner contract
  指向 Store 旧定义证明；GREEN 后 Local 行为 golden 5/5、Store↔Local identity 1/1、Store 旧 status 7/7、
  Put result 8/8、FlushDiskType 8/8、DefaultMappedFile compaction 1/1、legacy adapter 9/9。M06 contract
  87/87，通过 focused source-contract mutation 覆盖 Status、Result、Context、compaction trait、Flush 与禁边
  六类语义漂移。
- [x] `[REVIEW]` Local default/no-default/fast/safe/fast+safe/all 六组与 Store default/all、真实
  `local_file_store` owner 的 fast/safe/fast+safe 五组均通过。四个 raw Store no-default 组合仍各为既有
  `exit 101`、124 个 E0004 与 1 个 unused `ArcMut`；BASE 对 root/Store manifest 与 `message_store.rs` 零 diff。
  Local normal dependency tree 共 67 个包且无 common/remoting/store/rust/broker 禁边；四个 standalone Cargo
  project 无 Local/Store 直连，`rocketmq-store-inspect` 属 root workspace。
- [x] `[PLATFORM]` WSL/Linux 隔离 target 通过 Local focused 5/5、Store identity 1/1 与 Local/Store default
  checks；`cargo clean --target-dir target/wsl-m06-03o` 删除 8,480 files/4.4 GiB，并确认隔离目录不存在。
- [x] `[REV]` Local/Store package 与 root workspace all-target/all-feature Clippy、Local strict Rustdoc、Store
  normal Rustdoc、格式/diff、AGENTS routing、架构 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+initial/
  final guard 全部通过。BASE `1da5f8d638b5e84fc6808e31a53c6d994b630fe9` detached scan→candidate 直审均为
  1,232 identities/3,375 occurrences，0 NEW/0 STALE/0 line-only/0 semantic changes；canonical baseline 未改。
- [x] `[SCOPE]` M06-03o 不迁移 `AppendMessageCallback`、Put/Get status/result、完整 `MappedFile` trait、
  `DefaultMappedFile`、per-file flush primitive、flush manager 或 group commit。完整 mapped-file owner 迁移仍以
  消除/封装 `ArcMut` 和将 common broker/batch message 类型迁入 model 或建立中立 bridge 为硬前置；Local 不得
  临时依赖 common 或 rocketmq-rust。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03p memory-lock manager and platform syscall evidence

- [x] `[DEV]` `rocketmq-store-local::base::memory_lock_manager` 现唯一拥有 `MemoryLockManager`、
  `MemoryLockCategory` 与 `MemoryLockHandle`；`rocketmq-store-local::utils::ffi` 现唯一拥有 `mlock`/`munlock`
  平台实现。Local 新增普通 `rocketmq-error` 与可选 `rocketmq-observability` 依赖，并以 `observability` feature
  精确开启 `otel-metrics`；Store 的同名 feature 向 Local 转发。
- [x] `[COMPAT]` Store 两个旧模块仅 direct exact re-export 已迁移的三个类型和两个函数；页大小、madvise、
  prefetch、mincore 与常量仍由 Store 拥有。M06-03p 交付时三个跨 crate seam 暂时保持 `#[doc(hidden)] pub`，
  分别供当时的 Store `TransientStorePool`、`DefaultMappedFile` range-lock 与 CommitLog active-lock 生产 adapter
  使用；M06-03q 移除最后一个 `lock_buffer_with` 跨 crate caller 后将其收窄为 `pub(crate)`，其余两个 seam
  继续仅为 Store production adapter 临时公开。错误文本、strict/warn-only 预算语义、指标 label、计数器更新
  与原子内存序保持不变。
- [x] `[TEST]` TDD RED 先由缺失的 Local owner 文件与 feature/dependency contract 失败证明；GREEN 后 Local
  行为/并发 golden 5/5、Store↔Local 类型与函数 identity 2/2、Store 既有 pool/range/active-lock focused
  tests 4/4、2/2、2/2，Local 全量 161 项通过。Reviewer follow-up RED 证明旧的整文件粗 allowlist 无法拒绝新增
  生产 seam 调用；按 production/test 调用点收紧后，最终 M06 mutation-resistant contract 92/92。
- [x] `[FEATURE]` Local default/no-default/fast/safe/fast+safe/observability/all 七组 check 与 Store 七组受支持
  check 通过；依赖树证明 observability 关闭时 Local 不引入 telemetry，开启时精确进入 `otel-metrics`，Store
  转发到 Local。Observability CI 的七组 check/test 通过，六组 feature Clippy 与 default Clippy 均只复现两个
  未触及的 Broker 测试 unused-import 基线。
- [x] `[PLATFORM]` Windows 与 WSL/Linux 隔离 target 均通过 Local lock golden、Store identity 和
  observability 编译；Linux 条件编译审查将 Store 的 Windows-only error import 收窄为 `#[cfg(windows)]`。
- [x] `[REV]` Local/Store package Clippy、精确 root workspace all-target/all-feature Clippy、Local strict
  Rustdoc、架构 35 项+fixtures+baseline、AGENTS routing、ArcMut 63 项+24 fixtures+guard、格式与 diff 检查通过。
  Seam contract 分离 production/test 调用点并精确锁定逐文件次数与 canonical manager/Store adapter 共 11 个
  生产委托形态；mutation 会拒绝新增生产/测试调用、错误 receiver 及绕过 canonical manager 的
  syscall/callback 调用。
  Observability CI 的 default+六个指定 feature Clippy 探针只复现两个未触及的 Broker `DataVersionExt`
  unused-import 基线；error hygiene 仅复现未触及的 Broker/MCP 及缺失既有文档基线。
- [x] `[SCOPE]` M06-03p 不迁移或修复 `TransientStorePool`，也不迁移 `DefaultMappedFile`、CommitLog
  orchestration、flush/group commit、CQ/Index、HA、Timer/POP 或持久格式。PR-M06-03 父项、M06 Exit Checklist
  和 M06-04..12 保持未完成。

## M06-03q TransientStorePool ownership evidence

- [x] `[DEV]` `rocketmq-store-local::base::transient_store_pool::TransientStorePool` 现为唯一 canonical
  owner；Store 原模块只保留对该类型的直接精确 `pub use`，没有 wrapper、type alias 或第二份实现。
- [x] `[COMPAT]` 保留公开构造、初始化/销毁、借还 buffer、可用数量、memory-lock 统计和 real-commit
  getter/setter 的签名与类型身份。跨 crate fixture 证明 Store 旧路径与 Local 新路径可双向赋值，并共享
  queue 与 real-commit 状态。`lock_buffer_with` 已在最后一个 Store production caller 消失后收窄为
  `pub(crate)`；只有 Local canonical pool 是其外部于 manager 的 production caller，`lock_region_with` 与
  `unlock_region_with` 因 Store production adapter 仍保持 `#[doc(hidden)] pub`。
- [x] `[SEMANTICS]` 原有生命周期语义未被顺带修复：重复 `init` 继续追加；借出使用 `pop_front`，归还允许
  任意长度和超容量并使用 `push_front`；告警阈值仍为 `pool_size / 10 * 4`。`destroy` 只 drain 当前 available
  queue，对每个 buffer 直接调用 `munlock(ptr, file_size)`，包括 lock 失败或预算跳过的 buffer；首个 unlock
  错误停止后续 syscall，但 drain drop 仍清空剩余 queue；借出的 buffer 不处理，manager 统计不回退。
- [x] `[TEST]` TDD RED 先由缺失 Local owner/module 和 Rust public fixture E0432 证明；GREEN 后 Local 公开
  Clone/共享状态/并发归还契约 2/2、内部生命周期 8/8、Store 类型身份 1/1、旧 Store mapped-file round-trip
  1/1、完整 Local 171 项及 M06 source/mutation contract 95/95 通过。独立审查 follow-up RED 证明旧 contract
  会放过 `destroy_with_unlocker` 的 `pub(crate)`/`pub(super)`/`pub(in ...)` 及额外 production/test caller；GREEN
  后收紧直接调用面。第二轮 follow-up RED 进一步证明仅匹配后续 `(` 会放过
  `Self::destroy_with_unlocker` 等函数项/alias reference；最终 contract 扫描去注释/字符串后的全部
  `.destroy_with_unlocker`/`::destroy_with_unlocker` reference 且不计函数声明，精确要求 production 仅在公开
  `destroy` 中有一个 reference、四个命名模块测试各一个、其他 Local/Store production/test 文件为零。
  mutation contract 同时拒绝 owner copy、facade wrapper、共享字段/API/阈值/queue 顺序/drain/unlock/
  real-commit/Drop 漂移、任何 `pub` destroy seam visibility、额外直接 caller 与函数项/alias reference。
- [x] `[FEATURE]` 未修改 Cargo manifest、feature 或依赖；Local 与 Store 各七组 feature closure、default/all
  package Clippy、Local strict Rustdoc 均通过。Store 普通 Rustdoc 仅复现 4 个未触及的 invalid-HTML warning。
- [x] `[PLATFORM]` Windows 完成全部 focused/full 验证；WSL/Linux 隔离 target 通过公开契约 2/2、内部语义
  8/8、Store identity 1/1 与 Local/Store observability 编译。隔离目录已清理 9,872 files/4.8 GiB 并确认不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard 与 AGENTS routing
  均通过。error hygiene 只复现未触及的 Broker/MCP 与两份缺失治理文档基线；本切片没有 runtime、错误架构、
  manifest、feature 或 ArcMut 变更。M06 contract 精确拒绝 Store 或第二个 production caller 调用
  `lock_buffer_with`，也拒绝将其重新扩大为跨 crate public seam。
- [x] `[SCOPE]` M06-03q 只迁移 `TransientStorePool` owner；不修复其既有 lifecycle/accounting 行为，不迁移
  `MappedFile`/`DefaultMappedFile`、CommitLog orchestration、flush/group commit、CQ/Index、HA、Timer/POP、
  runtime ownership 或持久格式。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03r mapped-file page/progress threshold policy evidence

- [x] `[DEV]` `rocketmq-store-local::mapped_file::kernel` 现为固定页阈值策略的 canonical owner：公开
  `OS_PAGE_SIZE: u64 = 1024 * 4`，并由 `MappedFileProgress::is_able_to_flush` 与
  `MappedFileProgress::is_able_to_commit` 执行判定。Store 原 `default_mapped_file_impl::OS_PAGE_SIZE` 路径仅
  直接精确 `pub use` Local 常量；两个 Store 私有 helper 只委托 Local progress policy，实际 flush/commit I/O
  仍由 Store 拥有，`StoreCheckpoint` import 保持不变。
- [x] `[COMPAT/SEMANTICS]` 完整冻结旧语义：固定 4096-byte 页；full segment 无条件可推进；正阈值使用
  `(source - progress) / 4096 >= least_pages` 的原生 `i32` 算术；零或负阈值退化为 `source > progress`；flush
  source 仍是 read position，commit source 仍是 wrote position。4095/4096 与 8191/8192 边界、相等/倒退
  progress、零/负阈值和 full short-circuit 均有回归覆盖；没有 checked/saturating 算术或动态 OS page lookup。
- [x] `[TEST]` TDD RED 先记录 Local focused Rust fixture 的 18 个缺失常量/方法编译错误，以及 source
  contract 的 11 个 owner/adapter 违规；GREEN 后 Local focused 8/8、Store 旧常量路径 1/1、既有
  `DefaultMappedFile` focused 30/30、完整 Local 173 项、Store lib 568/568 和 M06 source/mutation contract
  97/97 通过。mutation contract 拒绝常量类型/表达式、full short-circuit、正/非正分支、比较符、flush read
  source、commit wrote source、saturating/dynamic page、Store wrapper/re-export及额外 caller 漂移。
- [x] `[REV-FOLLOWUP]` 独立审查 follow-up RED 先证明旧 contract 会放过两个 Store wrapper 的 plain/restricted
  `pub`、Local/Store duplicate/cfg/cfg_attr decoy 和 split-helper raw policy；更强的 11-case RED 覆盖
  `4096`、`1024 * 4`、`4 * 1024`、`0x1000`、`1 << 12`、`2048 * 2`、local let/const、chained/module alias、
  renamed threshold argument，以及 nested cfg module decoy + active `impl ... where`。GREEN 后 Local/Store
  policy method 均须全 production 与 top-level inherent 各恰好一个定义、无 method/impl `cfg`/`cfg_attr`、
  签名/body 精确；Store wrapper 还须无任何 `pub` visibility。安全整数表达式 resolver 解析 fixed-page
  alias chain，并拒绝 DefaultMappedFile production 中 divisor resolve 为 4096 的 page division，同时不误伤
  合法 `/ 2` 或动态 `/ page_size` alignment；全 production reference map 继续拒绝额外 caller/reference。
- [x] `[REV-FINAL]` 最终审查发现 fixed-page arithmetic scanner 仍只覆盖 `default_mapped_file_impl.rs`；
  contract-first RED 复现了两个 Store 外部文件 raw helper 及一个跨文件 renamed constant alias 的三处逃逸。
  GREEN 将 divisor/alias 扫描扩展到全部 `rocketmq-store/src/**/*.rs` production source；精确屏蔽
  `#[cfg(test)]` item 而不截断其后的 production code，跨文件传播解析值为 4096 的 alias，并在每条违规诊断中
  携带文件路径。threshold-name comparison 仍只检查 DefaultMappedFile，避免误伤 Store 合法 orchestration。
  `StoreCheckpoint` 是唯一兼容例外：必须精确保留旧路径 import、`file.set_len(OS_PAGE_SIZE)?;` 及恰好两处常量
  引用，禁止承载 threshold algorithm；mutation 同时证明合法 `/ 2`、动态 `/ page_size` 和全 Store 精确 Local
  policy reference map 均未回归。
- [x] `[REV-POST-TEST]` 后续审查发现 Local owner 与 Default wrapper 唯一性检查仍沿用“首个 `mod tests` 前
  截断”，与已使用 item-span masking 的 arithmetic/reference 检查不一致。contract-first RED 对 Local/Default
  两个 policy method 分别复现 method `cfg`/`cfg_attr`、impl `cfg`/`cfg_attr`、active duplicate 和 active
  `impl ... where`，共 24 个 post-test production 逃逸；另有 4 个正向 decoy 证明精确 `#[cfg(test)]` impl 应被
  忽略。GREEN 后 canonical Local 与 Default 各只生成一份 masked production view，并统一供常量/re-export、
  global definition count、inherent impl/method、签名/body、fixed-page arithmetic 与 reference map 使用；测试
  item 被屏蔽，其后的 production item 仍参与合同审计。
- [x] `[FEATURE/PLATFORM]` 未修改 Cargo manifest、feature 或依赖。Local 与 Store 各七组 feature closure、
  default/all-feature all-target Clippy、root workspace all-feature Clippy 和 Local strict Rustdoc 均通过；Store
  普通 Rustdoc 仅复现 4 个未触及的 invalid-HTML warning。WSL/Linux 隔离 target 通过 Local focused 8/8、
  Store 旧路径 1/1 及两 crate all-feature check；隔离目录已清理 9,397 files/6.1 GiB 并确认不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard 与 AGENTS routing
  均通过。error hygiene 仅复现未触及的 Broker source-stringification、MCP anyhow 和两份缺失治理文档基线；
  本切片没有 runtime、unsafe、错误架构、manifest、feature、ArcMut、observability 或持久格式变更。
- [x] `[SCOPE]` M06-03r 只迁移 mapped-file 固定页/进度阈值 policy，不迁移 `MappedFile` trait 或
  `DefaultMappedFile` owner/factory/builder，不迁移 config、flush/group-commit I/O、CQ/Index、HA、Timer/POP，
  不改变 runtime ownership 或持久格式。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03s mapped-file memory-lock range policy evidence

- [x] `[DEV]` `rocketmq-store-local::mapped_file::kernel::MappedFileProgress::lock_region_range` 现为 mapped-file
  内存锁范围裁剪的 canonical owner，返回平台宽度的 `(offset, len)`；Store 私有
  `DefaultMappedFile::lock_region_address_and_len` 只精确委托 Local，再将返回 offset 通过 mapped-file base pointer
  的 `wrapping_add` 投影为地址。Store 仍拥有实际 `mlock`/`munlock` 调用和 memory-lock manager 交互。
- [x] `[COMPAT/SEMANTICS]` 完整冻结旧机械语义：`requested_len == 0` 或 `offset >= file_size` 返回 `None`；
  remaining 使用 `file_size.saturating_sub(offset)`；len 使用
  `min(requested_len, usize::try_from(remaining).unwrap_or(usize::MAX))`；裁剪后零长度返回 `None`；最后才以
  `usize::try_from(offset).ok()?` 转换 offset。Store 只对 Local 返回的 offset 使用 `as_ptr().wrapping_add`，传给
  manager 的是裁剪后 len，不是请求 len。
- [x] `[TEST]` TDD RED 先记录 source contract 的 6 个 owner/adapter 违规和 Local focused fixture 的 6 个缺失
  方法编译错误，明确证明 Local 尚无 owner 且 Store 仍保留裁剪算法。GREEN 后新增 2 个 focused
  source/mutation contract、Local range fixture 1/1、Store `DefaultMappedFile` 30/30 与 `lock_region` 2/2 通过；
  完整 M06 contract 99/99，Local 全量 174 项通过，另 9 个既有 doctest ignored。
- [x] `[CONTRACT]` contract 复用既有 item-span masking 与 inherent-method 解析，精确约束 Local 公开签名/body、
  Store 私有 wrapper body、唯一 production owner、唯一 Store caller/reference，并禁止 boundary adapter 重复
  range clipping。mutation 覆盖零长度、边界比较、min/max、`saturating_sub`、remaining
  与 offset 的 `usize` 转换、转换顺序、`wrapping_add`、requested/clamped len、method/impl
  `cfg`/`cfg_attr`、duplicate、post-test production 以及跨文件 caller/算法回流。
- [x] `[REV-FOLLOWUP]` 独立审查发现防回流扫描只检查 Store 且绑定变量名 `offset`；contract-first RED 证明
  Local 跨文件完整复制和 Store 将 offset/file-size/request/remaining/len 全部改名后均得到空违规列表。GREEN
  将扫描扩展至 `rocketmq-store-local/src` 与 `rocketmq-store/src` 全部 masked production function/method item，
  以变量角色和赋值关系归一化验证 file-size boundary、`saturating_sub`、request `min`、remaining
  `usize::try_from(...).unwrap_or(usize::MAX)`、offset `try_from(...).ok()?` 与最终 `(offset, len)` tuple dataflow；
  canonical kernel exact owner 是唯一允许的完整 policy，Store exact adapter 仍只允许 Local delegation。
  mutation 进一步覆盖 Local/Store 改名复制、带/不带零长度 guard、跨文件 remaining helper、direct/use-alias、
  helper-owned 或 caller-owned boundary、`cfg`/`cfg_attr` impl、`impl ... where`、post-test active item 与纯测试
  decoy；独立 `saturating_sub`/`min` 正例证明不会误伤不完整的合法算术。增强后 focused 2/2 和 full M06
  contract 99/99 通过；inline duplicate 诊断携带其文件路径，split-helper 诊断同时携带 caller/helper 路径。
- [x] `[REV-GUARD-DATAFLOW]` 第二轮独立审查证明第一轮 scanner 的 boundary 与 return 仍绑定表面语法；
  contract-first RED 分别复现 request/offset 两个独立 `None` guard、带多层无副作用括号的 combined guard、
  `file_size <= offset` 反向边界，以及 `(offset, len)` 经单层 tuple binding 后 `Some(result)`，四个完整等价
  Local production copy 均得到空违规列表。GREEN 新增共享 guard parser：先按 balanced parentheses 提取
  production `if` item 的精确 `return None` early-exit，再拆 top-level OR/comparison，将 combined/separate guard、
  `offset >= file_size`/`file_size <= offset` 和任意无副作用外层括号归一到 request/offset/file-size 角色；inline、
  split caller 与 remaining helper 共用同一分析。return dataflow 支持 direct `Some((offset, len))`、单层及有界
  链式 tuple alias 后再 `Some(alias)`，仍精确绑定转换后的 offset 与裁剪后 len；缺少 len guard 的复制继续被
  拒绝。mutation 同时覆盖 Local/Store、cross-file helper/alias、post-test active item 和测试 decoy；focused 2/2、
  full M06 contract 99/99 通过，合法不完整近似仍保持零违规。
- [x] `[FEATURE/PLATFORM]` 未修改 Cargo manifest、feature 或依赖。Windows 上 Local 与 Store 各七组 feature
  closure、两 crate all-target/all-feature Clippy、root workspace all-target/all-feature Clippy、Local strict
  Rustdoc 均通过；Store 普通 Rustdoc 仅复现 4 个未触及的 invalid-HTML warning。Windows 隔离 target 通过
  Local 1/1、Store 2/2 及两 crate all-feature check，清理 9,537 files/8.6 GiB。WSL/Linux 隔离 target 的
  Local 1/1、Store 2/2、Local/Store all-feature check 均通过；组合脚本只在全部验证结束后的 cleanup 阶段因
  target-dir 变量为空以 exit 1 结束，验证本身均已通过。随后固定路径执行
  `cargo clean --target-dir /tmp/rocketmq-m06-03s-wsl` 成功，删除 9,381 files/6.2 GiB，并确认该路径不存在；
  Windows 隔离路径也确认不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing 与
  workspace fmt/diff 检查均通过。error hygiene 仅复现未触及的 Broker source-stringification、MCP anyhow 和
  两份缺失治理文档基线；本切片没有 runtime、unsafe、error mapping、manifest、feature、ArcMut、动态页/锁
  accounting、strict/warn 行为、CommitLog active-lock lifecycle 或持久格式变更。
- [x] `[SCOPE]` M06-03s 只迁移 `DefaultMappedFile::lock_region_address_and_len` 的纯裁剪 policy；既有
  `MemoryLockManager::lock_region_with`/`unlock_region_with` hidden public seam、CommitLog active-lock
  orchestration、flush/group commit、CQ/Index、HA、Timer/POP 均保持未完成。PR-M06-03 父项、M06 Exit
  Checklist 和 M06-04..12 保持未完成。

## M06-03t mapped-file warmup schedule policy evidence

- [x] `[DEV]` `rocketmq-store-local::mapped_file::kernel::visit_mapped_file_warmup_schedule` 现为 mapped-file
  warmup 调度的 canonical owner，以无分配 visitor 依次产出 `MappedFileWarmupOperation::Touch` 与 `Flush`。
  Store `DefaultMappedFile::warm_mapped_file_with_ops` 只把 Local operation 投影为实际 `MmapMut` touch/flush；
  `get_page_size`、`FlushDiskType`、madvise、错误/降级事件、warning、flush timestamp、metrics、文件身份和生命周期
  仍归 Store 所有。
- [x] `[COMPAT/SEMANTICS]` 完整冻结旧机械顺序：空文件零操作；`page_size` 与 `flush_every_pages` 均在 Local
  `max(1)`；每个 `(0..file_size).step_by(page_size)` offset 先 Touch 再累计页数；Sync 每逢
  `is_multiple_of(flush_every_pages)` 产生 periodic Flush，end 仍精确为 `(offset + 1).min(file_size)`，不是页尾；
  schedule 不因 Store 真实 flush 成败改变 last offset；循环后仅 Sync 对 remainder 产生 final Flush；Async 只 Touch。
  `Flush.final_flush` 区分 periodic 与 remainder，使 Store 分别保留旧 `Failed to flush warmed...` 和
  `Failed to flush final warmed...` warning，真实 flush、降级 event 与成功 record-time 顺序不变。
- [x] `[TEST]` contract-first RED 先由 source contract 证明 Local owner/Store delegation 缺失，并由 Local fixture
  复现两个 unresolved import 编译错误。GREEN 后 Local 精确序列 4/4、Store degradation/position 旧路径 1/1、
  `DefaultMappedFile` 30/30、Local default 与 all-feature 全量各 178/178（另各 9 doctest ignored）通过；完整
  M06 source/mutation contract 从 99 增至 101 项并 101/101 通过。
- [x] `[CONTRACT]` contract 精确约束公开 Copy/Eq operation enum、visitor 签名/body、唯一 production owner、
  无 `Vec`/`collect` 分配、Store 唯一 caller/import/match adapter、Touch/Flush 参数和两种 warning 词汇，并禁止
  Store production 保留 `step_by`、`is_multiple_of`、touched/last/flush-every 调度角色。mutation 覆盖空文件、
  page/interval 归一化、touch 计数与顺序、sync/async、旧 `offset + 1`、periodic/final range 与标志、最终 remainder、
  enum/function `cfg`/`cfg_attr`/duplicate/post-test decoy、Store 重复归一化/错误 flush 参数/反转 final flag/warning
  词汇，以及跨 Store 文件复制 schedule token；合法测试 decoy 保持零误报。
- [x] `[FEATURE/PLATFORM]` 未修改 Cargo manifest、feature 或依赖。Windows 隔离 target 上 Local 七组和有效
  Store 七组 feature closure、两 crate package Clippy、root workspace exact all-target/all-feature Clippy、Local
  strict Rustdoc 均通过；Store normal Rustdoc 仅复现 4 个未触及的 invalid-HTML warning。额外 Store
  `--no-default-features` 空后端探测仍复现既有 124 个 E0004 与 unused `ArcMut` 基线，未标记为通过。WSL/Linux
  隔离 target 通过 Local 4/4、Store 1/1 及两 crate all-feature check；固定路径 cleanup 成功，删除 10,101 files/
  6.5 GiB 并确认 target 不存在。Windows 隔离路径也清理 25,573 files/21.7 GiB 并确认不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing、workspace
  fmt/diff 检查均通过。error hygiene 仅复现未触及的 Broker source-stringification、MCP anyhow 和两份缺失治理
  文档基线；本切片没有 runtime ownership、unsafe、public error mapping、manifest、feature、ArcMut、动态内存锁
  accounting 或持久格式变更。
- [x] `[SCOPE]` M06-03t 只迁移 `DefaultMappedFile` warmup 的纯 operation schedule；实际 mapped-memory I/O、
  flush/group commit orchestration、CQ/Index、HA、Timer/POP 仍保持未完成。PR-M06-03 父项、M06 Exit Checklist 和
  M06-04..12 保持未完成。

## M06-03u normal recovery file-window planning evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::recovery::plan_normal_recovery_file_window` 现为 normal
  recovery mapped-file 扫描窗口的 canonical owner；公开的 `NormalRecoveryFileWindow` 只携带 `start_index` 与
  `file_count_limit`。Store 删除旧默认常量和两个 helper，standard/optimized 两条 normal recovery 各直接调用
  planner 一次；abnormal recovery 不消费该 planner。
- [x] `[COMPAT/SEMANTICS]` 完整冻结旧策略：配置值 `0` 的 effective limit 仍为 `3`，显式非零值原样保留，
  start index 仍为 `mapped_file_count.saturating_sub(effective_limit)`；limit 大于文件数时 start 为 `0`，日志仍报告
  未裁剪的 effective limit。两条 normal recovery 的日志文本/参数、循环、parser/state、统计、写回和 standard
  尾部 `recovery_file_limit` 诊断保持不变；abnormal planner 的 `0` 语义没有被复用。
- [x] `[TEST]` contract-first RED 先由 Local fixture 复现两个 unresolved import，并由 source contract 证明
  canonical 文件缺失；GREEN 后 Local window fixture 1/1、Store normal-recovery unit 3/3 与 integration 12/12、
  Store lib 566/566 通过。Local default/all-feature 全量均通过；完整 M06 source/mutation contract 从 101 增至
  103 项并 103/103 通过。
- [x] `[CONTRACT]` contract 固定唯一 Local owner、公开 Copy/Eq struct、私有默认值、planner 签名/无分配纯
  dataflow、`0` fallback、显式 limit 与 saturating subtraction；Store 只允许一条精确 import 和两条 direct call，
  强制 index/limit 字段绑定、两条 start log 和 standard 尾部 limit 消费，并禁止旧常量/helper、Store 构造 window、
  abnormal 误用及跨 Store production 复制。mutation 覆盖默认值、zero 分支、原值、减法/操作数、off-by-one、字段
  交换、只一路委托、手工构造、异常路径误用、rename/alias/split helper、cfg/cfg_attr/duplicate/post-test decoy；
  `#[cfg(test)]` 内同名测试 decoy 保持零误报。
- [x] `[FEATURE/PLATFORM]` 未修改 Cargo manifest、feature 或依赖。Windows 固定隔离 target 上 Local/Store 两 crate
  7+7 有效 feature closure、两 crate package Clippy、root workspace exact all-target/all-feature Clippy 与 Local
  strict Rustdoc 均通过；Store normal Rustdoc 只复现 4 个未触及的 invalid-HTML warning。Windows 固定路径 cleanup
  删除 23,680 files/24.1 GiB 并确认 target 不存在。WSL/Linux 固定隔离 target 通过 Local 1/1、Store recovery
  12/12 及两 crate all-feature check；固定路径 cleanup 删除 9,539 files/6.4 GiB 并确认 target 不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing、workspace
  fmt/diff 检查均通过。error hygiene 只复现未触及的 Broker source-stringification、MCP anyhow 和两份缺失治理
  文档基线；本切片没有 runtime ownership、unsafe、public error mapping、manifest、feature、ArcMut、I/O、
  observability、flush/group commit 或持久格式变更。
- [x] `[SCOPE]` M06-03u 只迁移 normal recovery 文件扫描窗口纯 planner；config owner、mapped-file I/O、
  parser/recovery state、错误/可观测性、async/runtime、flush/group commit、CQ/Index、HA、Timer/POP 均未迁移。
  PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03v recovery ConsumeQueue truncation policy evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::recovery::should_truncate_recovery_consume_queue` 现为
  recovery 后 ConsumeQueue truncate 纯判定的唯一 canonical owner，并由 `recovery.rs` 直接精确 re-export。
  Store 删除 normal/abnormal 两个同义私有 helper 与两份重复单测；standard/optimized 的 normal/abnormal 四条
  recovery 路径各直接调用同一个 Local 函数一次，不保留 wrapper、alias 或第二份实现。
- [x] `[COMPAT/SEMANTICS]` 完整冻结旧判定：任意负 `max_phy_offset`（含 `i64::MIN`）均返回 `true`；非负值只经
  `u64::try_from` 后按 `>= truncate_offset` 判断。`0/0` 为 true，`0/10`、`9/10` 为 false，`10/10`、`11/10`
  为 true，`i64::MAX/u64::MAX` 为 false；未改为严格大于、反向比较、负值 false、`as` 强转或将 truncate
  先收窄到 `i64`。四条调用附近 warning 文本/参数、`truncate_dirty_logic_files(process_offset)`、水位写回及
  其余 normal/abnormal recovery 编排保持不变，M06-03u normal window planner 未受影响。
- [x] `[TEST]` contract-first RED 先由缺失 canonical 文件和 Store direct adapter 产生 source contract 失败，
  Local fixture 同时复现 unresolved import；GREEN 后新增 Local signed-boundary fixture 1/1、Local recovery
  state/window/truncation 18/18、Store recovery focused 29/29、integration 19/19 与 Store lib 564/564 通过。
  Local default/all-feature 全量均通过；完整 M06 source/mutation contract 从 103 增至 105 项并 105/105 通过。
- [x] `[CONTRACT]` contract 固定唯一 Local owner、公开精确签名/body、`recovery.rs` module/re-export、无
  Store/config/I/O/log/async/alloc 依赖；Store 只允许一条直接精确 import 和四条 direct call，并锁定 normal/
  abnormal、standard/optimized 分布、参数角色与 `if` 控制流，同时禁止两个旧 helper、额外 caller 和 Store
  算法副本。mutation 覆盖负值分支、等值边界、比较方向/操作数、`try_from`/unchecked cast、wrong argument、
  只迁一路/额外调用、import alias、cfg/cfg_attr、duplicate、post-test production、变量改名完整复制以及
  direct/use-alias/split helper 复制；仅含负值、转换或阈值片段的不完整近似保持零误报。
- [x] `[FEATURE/PLATFORM]` 未修改 Cargo manifest、feature 或依赖。Local 与 Store 各七组有效 feature closure、
  两 crate default/all-feature package Clippy、root workspace exact all-target/all-feature Clippy 和 Local strict
  Rustdoc 均通过；Store normal Rustdoc 只复现 4 个未触及的 invalid-HTML warning。Windows 固定隔离 target
  通过 Local 1/1、Store recovery 19/19 及两 crate all-feature check，cleanup 删除 9,698 files/9.1 GiB 并确认
  路径不存在；WSL/Linux 同一矩阵通过，cleanup 删除 9,543 files/6.4 GiB 并确认路径不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing 与
  workspace fmt/diff 检查均通过。error hygiene 只复现未触及的 Broker source-stringification、MCP anyhow 和
  两份缺失治理文档基线；本切片没有 runtime ownership、unsafe、public error mapping、manifest、feature、
  ArcMut、I/O、observability、flush/group commit 或持久格式变更。
- [x] `[SCOPE]` M06-03v 只迁移 recovery ConsumeQueue truncate 纯判定；config owner、mapped-file I/O、
  parser/recovery state/window、错误/可观测性、async/runtime、flush/group commit、CQ/Index、HA、Timer/POP 均未
  迁移。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03w abnormal recovery confirm-candidate checked calculation evidence

- [x] `[DEV]` 新增
  `rocketmq-store-local::commit_log::recovery::abnormal_confirm_candidate_end` 与
  `AbnormalRecoveryConfirmCandidateError`，由 `recovery.rs` 直接精确 re-export，成为 abnormal recovery
  confirm-candidate checked calculation 的唯一 canonical owner。Store 删除私有
  `AbnormalRecoveryAdapterOffsetError`、同名 helper 与旧单测；optimized 路径继续传 raw `msg_size`，standard
  路径继续传 raw `input_size`，两处 warning 文本和 `break 'segments` 失败控制流保持不变，normal recovery 零调用。
- [x] `[COMPAT/SEMANTICS]` 完整冻结检查顺序与边界：先拒绝任意负 `commit_log_offset`（含 `i64::MIN`），再以
  `i64::try_from` 转换 raw `usize` input size，最后以 `checked_add` 求 end。`(7, 5)` 为 `12`；
  `(i64::MAX, 0)` 保持 `i64::MAX`；`(i64::MAX, 1)` 返回 overflow；64 位平台上 `i64::MAX + 1` 的 `usize`
  返回 size conversion error。三个 error variant 的字段及逐字 Display 保持 Store 旧 warning 的可见文本。
- [x] `[TEST]` TDD RED 先由 Local focused fixture 的两个 unresolved import 证明 owner/API 尚不存在；GREEN 后
  Local fixture 3/3、Store abnormal recovery integration 7/7、Store lib 563/563 通过，Local default 与
  all-feature 全量均通过。新增两项 owner/adapter mutation contract，连同既有 abnormal reducer focused 共
  3/3；完整 M06 source/mutation contract 从 105 增至 107 项并 107/107 通过。
- [x] `[CONTRACT]` contract 固定唯一 Local enum/function owner、公开签名、negative → size conversion →
  checked-add 顺序、operand/variant field、三条 Display、无 cfg/cfg_attr 与无 Store/config/I/O/log/async/alloc
  依赖；Store 只允许一条直接精确 import、optimized/standard 各一次 raw-input direct call、normal 零调用、无
  legacy owner/error。mutation 覆盖 `<` 边界、检查重排、`as`、plain/saturating/wrapping add、wrong operand/
  error field/variant、Display、只迁一路、额外/normal caller、alias/brace/cfg import、active post-test owner，及
  renamed、tuple-alias、direct/split helper 完整副本；不完整的独立 checked-add 近似保持零误报。
- [x] `[FEATURE/PLATFORM]` 未修改 Cargo manifest、feature 或依赖。Local 与 Store 各七组有效 feature closure、
  两 crate all-target/all-feature package Clippy、root workspace exact all-target/all-feature Clippy、Local strict
  Rustdoc 均通过；Store normal Rustdoc 只复现 4 个未触及的 invalid-HTML warning。Windows 固定隔离 target
  通过 Local 3/3、Store abnormal 7/7 与两 crate all-feature check；cleanup 删除 9,710 files/9.1 GiB 并确认
  路径不存在。WSL/Linux 同一矩阵通过；cleanup 删除 9,554 files/6.4 GiB 并确认路径不存在。额外 Store
  空 backend `--no-default-features` 误命令只复现既有空 enum 的 124 个 E0004 与 unused import，不计通过；两次
  未实际执行验证的 WSL wrapper 传参/PATH/CRLF 误命令同样不计通过，修正后的 login-shell 固定路径矩阵 exit 0。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing、
  workspace fmt/diff 检查均通过。error hygiene 只复现未触及的 Broker source-stringification、MCP anyhow 与
  两份缺失治理文档基线；本切片没有 runtime ownership、unsafe、public error mapping、manifest、feature、
  ArcMut、I/O、observability、flush/group commit 或持久格式变更。
- [x] `[SCOPE]` M06-03w 只迁移 abnormal recovery confirm-candidate 的纯 checked calculation；parser/recovery
  state/window、dispatch gate、warning/orchestration、config、mapped-file I/O、async/runtime、flush/group commit、
  CQ/Index、HA、Timer/POP 均未迁移。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03x CommitLog active memory-lock target planning evidence

- [x] `[DEV]` 新增 `rocketmq-store-local::commit_log::memory_lock`，由公开 Copy/Eq
  `CommitLogMemoryLockMode`、`CommitLogMemoryLockTarget` 与
  `plan_commit_log_memory_lock_target` 唯一拥有 active CommitLog memory-lock target 纯规划。Store 删除私有
  128 MiB 默认常量、target struct 与重复算法；`active_memory_lock_target_for_config` 只把
  `effective_linux_memory_lock_mode()` 精确映射为三个 Local mode，并把 window bytes、wrote position 与 file size
  各传入 planner 一次。实际 mapped-file 指针、mlock/munlock、budget/accounting、current-region 生命周期、错误与日志
  仍归 Store/既有 Local `MemoryLockManager` 所有。
- [x] `[COMPAT/SEMANTICS]` 完整冻结旧边界：任意 mode 的零长度文件均无 target；Off 无 target；ActiveWindow 在
  `wrote_position >= file_size` 时无 target，配置 window 为零时使用 Local 私有 128 MiB 默认值，否则保留配置值，
  remaining 继续 `saturating_sub`，长度继续以 `usize::try_from(remaining).unwrap_or(usize::MAX)` 裁剪，category/
  offset 分别为 active-window 与 wrote position。ActiveFile 继续以 `usize::try_from(file_size).ok()?` 保留 32 位
  fail-closed 语义，并产生 offset 0/active-file target；两路均拒绝零长度 target。Store mapped-file adapter 继续对
  signed wrote position 执行 `max(0) as u64`，LowLatency effective-mode 覆盖和 lock/unlock/current fast path 未改变。
- [x] `[TEST]` Local target fixture 3/3、Store effective-mode/present 2/2、active-file lifecycle/strict-init 2/2、
  Store lib 562/562、Local default 与 all-feature 全量均通过。新增 owner 与 Store adapter 两项 source/mutation
  contract；缺 owner/adapter、边界和复制 mutation 保留 RED 证据，GREEN 后 focused 2/2，完整 M06 contract 从
  107 增至 109 项并 109/109 通过。
- [x] `[CONTRACT]` contract 锁定 Local module、唯一公开 mode/target/planner owner、私有 128 MiB 常量、精确字段/
  签名/dataflow、无 alloc/I/O/log/async/config/Store 依赖；Store 只允许四条直接精确 import、一个 effective-mode
  config adapter、一个 planner call 和既有 Local target lifecycle flow。mutation 覆盖 default/zero/Off、window/file、
  wrote 等值与越界、remaining/min、两种 `try_from`、category/offset/len、effective-vs-raw mode、wrong config argument、
  额外/缺失 caller、cfg/cfg_attr/duplicate/post-test owner，以及变量改名、direct/split/use-alias 完整算法副本；仅含
  unrelated `min`/`try_from` 的合法近似保持零误报。
- [x] `[FEATURE/PLATFORM]` 未修改 Cargo manifest、feature 或依赖。Local 与 Store 各七组有效 feature closure、
  两 crate all-target/all-feature package Clippy、root exact workspace Clippy、Local strict Rustdoc 均通过；Store 普通
  Rustdoc 只复现 4 个未触及的 invalid-HTML warning。Windows 固定隔离 target 通过 Local 3/3、Store 2/2+2/2
  与两 crate all-feature check，cleanup 删除 9,516 files/8.6 GiB 并确认路径不存在；WSL/Linux 同一矩阵通过，
  cleanup 删除 9,361 files/6.2 GiB 并确认固定 `/tmp/rocketmq-m06-03x-wsl` 不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing、workspace
  fmt/diff 检查均通过。两个既有 `DefaultMappedFile` production occurrence 因 target 类型迁移和相邻 adapter 缩短
  发生同 item 一对一 fingerprint 变化，按 ADR-013 精确批准；promoted baseline 保持 1,232 identities/3,375
  occurrences。error hygiene 只复现未触及的 Broker source-stringification、MCP anyhow 与两份缺失治理文档基线。
- [x] `[SCOPE]` M06-03x 只迁移 CommitLog active memory-lock target 纯 planner；不修改 config owner、实际 mlock/
  munlock、budget/accounting、mapped-file I/O、unsafe、错误/可观测性、async/runtime、flush/group commit、CQ/Index、
  HA、Timer/POP 或持久格式。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03y mapped-file cache-residency range validation evidence

- [x] `[DEV]` `MappedFileProgress::is_valid_cache_range(position, size)` 现为 cache-residency 纯范围校验的唯一
  canonical owner；`DefaultMappedFile` 同名私有 helper 只精确委托一次。Linux `mincore` 页规划/分配、Windows 与
  macOS 平台 adapter、mapped-memory 指针、指标与 cache hit/miss 编排仍归 Store 所有，三个 `is_loaded` caller
  的 cfg 与调用流保持不变。
- [x] `[COMPAT/SEMANTICS]` 冻结 signed position 在转换前拒绝负值、零 size 拒绝、start 等于 file size 拒绝、
  end 等于 file size 接受、越界拒绝和 `checked_add` 溢出 fail-closed。file size 与 position 继续使用既有
  `as usize` 顺序和 32 位截断语义；未改为 `try_from`、`saturating_add` 或 `wrapping_add`，也未修改平台实际
  residency 探测算法。
- [x] `[TEST]` TDD RED 由 Local fixture 的 8 个 E0599 证明 owner 缺失；GREEN 后 Local cache-range focused
  1/1、完整 mapped-file kernel 14/14、Local lib 69/69 与 all-feature 全量通过，Store invalid-range 1/1 和
  `DefaultMappedFile` 30/30 通过。M06 source/mutation contract 新增 owner/adapter 两项，从 109 增至 111 项，
  focused 2/2 与完整 111/111（463.581s）均通过；独立审查修复 detached-boundary near miss 后再次完整运行
  111/111（416.541s）通过。
- [x] `[CONTRACT]` contract 锁定 Local 精确签名/body/statement order、唯一非 cfg owner、Store 私有 exact
  wrapper、Linux/Windows/macOS 三个 caller 和四个 production reference。mutation 覆盖 negative/zero/start/end、
  `checked_add`、wrong operands、overflow、`as usize`/`try_from`/saturating/wrapping、wrapper visibility/extra logic/
  missing caller、cfg/cfg_attr/duplicate/post-test，以及变量改名、alias chain、跨文件 direct copy 与 split-helper
  use-alias 重构；无 guard 的无关 `checked_add`、返回 true 的 zero-range，以及仅把 `position < file_size` 赋给
  未参与返回值的 `_observed_only` near miss 均保持零误报，起点边界必须与 `checked_add` 位于同一布尔链。
- [x] `[FEATURE/PLATFORM]` 未修改 manifest、feature 或依赖。Local 七组与 Store 七组有效 feature closure、两
  crate all-target/all-feature package Clippy、root exact workspace Clippy、Local strict Rustdoc 均通过；Store
  普通 Rustdoc只复现 4 个未触及的 invalid-HTML warning。裸 Store `--no-default-features` 继续复现既有空 backend
  enum 的 124 个 E0004 与 unused `ArcMut`，未标记为通过。Windows 固定隔离 target 通过 Local 1/1、Store 1/1
  和两 crate all-feature check，cleanup 删除 9,554 files/8.6 GiB 并确认路径不存在。WSL/Linux 同一验证均通过；
  首轮最后命令曾因 PowerShell 管道注入 `--all-features\r` 失败，cleanup 删除 6,737 files/3.6 GiB；以 LF base64
  wrapper 重跑 Store all-feature check 后通过，第二次 cleanup 删除 3,623 files/2.9 GiB，固定路径最终不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing、workspace
  fmt、Python compile 与 diff 检查均通过。一次误用 `arc_mut_guard.py --current-milestone M06` 只报告 642 个既有
  M05 过期 baseline，未计为 gate；正确无参数 final guard 通过。error hygiene 只复现未触及的 Broker source
  stringification、MCP anyhow 与两份缺失治理文档基线。
- [x] `[SCOPE]` M06-03y 只迁移 cache-residency 纯范围校验；不迁移或修改 `mincore`/VirtualQuery、page planning、
  allocation、pointer/unsafe、metrics、错误/可观测性、mapped-file I/O、async/runtime、flush/group commit、CQ/Index、
  HA、Timer/POP 或持久格式。PR-M06-03 父项、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03z Linux mapped-file cache-residency page-planning evidence

- [x] `[DEV]` `MappedFileCacheResidencyPlan` 与 `plan_mapped_file_cache_residency` 现为 Linux cache-residency
  对齐地址、检查长度和 residency 页数的唯一 canonical owner；Local 只处理数值型地址与范围，不拥有或解引用
  mapped-memory 指针。Store Linux adapter 保留 M06-03y 范围校验、`get_page_size`、mapped pointer 获取、`Vec`
  分配、`mincore`、residency 结果判定与 metrics 编排，并以 fully-qualified path 精确调用 planner 一次；Windows 与
  macOS adapter 未修改。
- [x] `[COMPAT/SEMANTICS]` 冻结既有 `position as usize`、`page_size.max(1)`、base+position 和 page-offset+size
  两处 `saturating_add`、向下 alignment、page offset、`div_ceil` 与零 page-count 返回 `None` 的顺序。极值输入保持
  不 panic；未在 planner 重复增加 zero-size/range validation，因此 aligned zero-size 返回 `None`、misaligned
  zero-size 仍产生既有非零原语义 plan，实际 Store caller 继续由 M06-03y 在调用前拒绝零 size。
- [x] `[TEST]` Rust TDD RED 由 Local fixture 的 2 个 E0432 证明 owner/type 缺失；GREEN 后 planner focused 1/1、
  mapped-file kernel 15/15、Local lib 69/69、Local default/all-feature 全量、Store invalid-range 1/1 与
  `DefaultMappedFile` 30/30 均通过。M06 source/mutation contract 新增 owner/adapter 两项，从 111 增至 113 项，
  初始 focused 2/2（32.781s）与审查前候选完整 113/113（429.103s）均通过。首次独立审查仍发现 helper 参数/tuple
  返回角色顺序漏检及 plan result 字段错接误报 2 个 Important；修复 TDD RED 由 swapped parameters、swapped return、
  两者同时的 3 个漏报与 scrambled fields 的 1 个误报证明。GREEN 后修复候选 z focused 2/2（36.206s）、y+z
  focused 4/4（61.891s）与完整 113/113（433.456s）均通过；独立技术复审再跑完整 113/113（435.787s）通过。
- [x] `[CONTRACT]` contract 锁定 plan struct 的 `Copy/Eq`、公开字段顺序/类型、planner 签名/body/statement order/
  visibility/cfg、唯一 Local owner，以及 Linux Store guard→page-size→base-address→单次 planner→allocation→`mincore`
  →result 的精确 dataflow。mutation 覆盖 cast/max、两处 saturating add、alignment/subtraction、`div_ceil`、zero guard、
  plan 字段映射与 Store 参数/字段消费，并覆盖 cfg/cfg_attr/duplicate/post-test、变量改名/alias、direct/cfg copy 和跨文件
  split-helper use-alias 完整副本。审查修复后 helper contract 结构化保留 start/page 参数索引与 aligned/offset tuple 返回
  索引，不依赖固定参数或返回顺序；result contract 从对应 3×`usize` struct 解析字段顺序和 shorthand/显式 initializer，
  逐字段绑定 aligned/checked-len/page-count 角色。swapped helper parameters/returns 及组合仍识别完整副本，正确显式字段
  仍命中，而 scrambled fields、单独 saturating-add/alignment/div-ceil 与缺少后续角色均保持零误报。
- [x] `[FEATURE/PLATFORM]` 未修改 manifest、feature 或依赖。Local 与 Store 各七组有效 feature closure、两 crate
  all-target/all-feature package Clippy、root exact workspace Clippy、Local strict Rustdoc 均通过；Store 普通 Rustdoc
  只复现 4 个未触及的 invalid-HTML warning。WSL/Linux 固定隔离 target 通过 Local planner 1/1、Store invalid-range
  1/1 和两 crate all-feature check，cleanup 删除 10,948 files/12.6 GiB 并确认固定路径不存在。
- [x] `[REV]` architecture 35 项+fixtures+baseline、ArcMut 63 项+24 fixtures+final guard、AGENTS routing、workspace
  fmt、Python compile 与 diff 检查均通过。初次 ArcMut final 因新增 import 的相邻上下文产生同 item 1 NEW/1 STALE；
  Store 改用 fully-qualified planner 调用后复核通过，baseline 与 relocation approval 保持零改动。error hygiene 只复现
  未触及的 Broker source stringification、MCP anyhow 与两份缺失治理文档基线。首次独立审查的 2 个 Important 已在
  `6ab8c390c` 修复；同一审查者对修复快照签署 Critical/Important/Minor = 0/0/0、`Approved`。
- [x] `[SCOPE]` M06-03z 只迁移 Linux cache-residency 纯整数 page planning；不迁移范围校验、实际 `mincore`、
  allocation、pointer/unsafe、metrics、错误/可观测性、mapped-file I/O、Windows/macOS adapter、async/runtime、flush/
  group commit、CQ/Index、HA、Timer/POP 或持久格式。PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和
  M06-04..12 保持未完成。

## M06-03aa CommitLog loader orchestration extraction evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::loader::CommitLogLoader` 现为 CommitLog 目录发现、全量 metadata
  校验、mapping plan、ordered parallel/sequential mapping、recovery hints、fully-loaded position 与 statistics
  归约的唯一 canonical owner。Local loader 状态保持非泛型，通过公开 `CommitLogLoadAdapter<T>` 函数表接收
  `open`、HRTB `recovery_mapping` 与 `mark_fully_loaded` 三个窄 target 操作，不依赖 Store representation。
- [x] `[COMPAT/SEMANTICS]` Store 保留同名 `CommitLogLoader` 薄 wrapper 和原有 `new`、
  `new_with_recovery_mmap_advice`、`new_with_recovery_hints`、`with_lazy_mmap`、`load_optimized` 调用面；wrapper
  只持有非泛型 Local loader。`load_optimized` 在旧 item 内以类型推断闭包适配 recovery/position，旧
  `create_mapped_file` item 负责 eager/lazy `DefaultMappedFile` open。缺失目录零 elapsed、空目录 elapsed、先校验后
  mmap、并行稳定顺序、最后文件 eager、hint 失败非致命与 position/statistics 语义保持不变。
- [x] `[TEST]` TDD RED 由 Local fixture 的 unresolved loader/adapter import 证明 owner/API 缺失；GREEN 后 Local
  fake-target fixture 3/3 与 Store loader compatibility/behavior 16/16 通过。M06 source/mutation contract 新增 Local
  owner 与 Store narrow-wrapper 两项，从 113 增至 115；按本切片范围运行 loader、mapping、hint、validation、
  discovery 的关联 contract/mutation 13/13 通过。主线程候选快照 gate 前两轮暴露 canonical 文件清单、Local loader
  别名边界和两处旧 Store mutation fixture 未同步；逐项修复并 targeted 复核后，完整 contract 最终 115/115 通过
  （444.612s）。
- [x] `[CONTRACT]` contract 锁定非泛型 Local loader 字段与 constructor defaults、函数表字段/HRTB、完整
  discovery→validation→plan→mapping→hint→position→statistics 顺序、并行 ordered collection/reduction、唯一 Local
  adapter/loader owner，以及 Store 五个公开委托、单次函数表构造与 `create_mapped_file` 映射。跨文件 copy guard
  拒绝 direct/use-alias/cfg/cfg_attr/post-test 的完整编排副本，`#[cfg(test)]` decoy 与缺少 summary 的 near miss 保持零误报。
- [x] `[FEATURE/PLATFORM]` 未修改 manifest、feature 或依赖。Local 与 Store 的 all-target/all-feature package
  Clippy 均以 `-D warnings` 通过；Local/Store focused behavior 在 Windows 通过，函数表只传递平台既有 mmap 与
  prefetch adapter，不新增 runtime、unsafe 或平台 I/O owner。
- [x] `[REV]` ArcMut 63 项、24 fixtures、ADR-013 monotonic promotion/compare 与更新 ledger 后的裸 final guard
  全部通过；ledger 保持 1,232 identities，occurrences 从 3,375 降至 3,372。tracked approvals 保留历史并追加 5 条
  同 item 一对一 fingerprint relocation；parallel、sequential 与 hint 三个旧 Store occurrence 已删除。Store 使用
  fully-qualified Local loader 路径，未放宽 mapped-file kernel alias guard。workspace fmt、Python compile 与 diff 检查
  通过，repo ArcMut guard/baseline schema 未放宽。
- [x] `[SCOPE]` M06-03aa 只迁移 CommitLog loader 完整编排；不迁移 `DefaultMappedFile` representation、实际 mmap/
  prefetch 平台执行、CommitLog append/recovery state、flush/group commit、CQ/Index、HA、Timer/POP、runtime ownership
  或持久格式。PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和 M06-04..12 保持未完成。
