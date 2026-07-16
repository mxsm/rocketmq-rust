# M06：Store API、Local 与 RocksDB 边界提取

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 进行中；M06-01/M06-02/PR-M06-03/PR-M06-04/PR-M06-05/PR-M06-06/PR-M06-07/PR-M06-08 已完成，继续 PR-M06-09 |
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

- [x] 入口：`[ARCH]` 确认 M02 `try_flush` 契约和 recovery golden 已冻结；本 PR 不搬 flush/group-commit，也不修改恢复行为。
- [x] `[DEV]` 创建 `rocketmq-store-local`，`default = []`，拥有 fast-load/safe-load/io_uring。
- [x] `[DEV]` 机械迁移 CommitLog append/load/recovery、MappedFile 与所需最小 config；store 旧深路径精确 re-export。
- [x] `[TEST]` focused test：dirty-tail truncate、CRC、segment roll、load/recovery 和 crash-before-flush golden。
- [x] `[REV]` 检查唯一 CommitLog owner、文件格式不变，Local 不依赖 Rocks/Tiered/store facade/Broker/remoting。
- [x] 回滚点：store compatibility facade、旧 public path、feature 入口与磁盘数据不变；可整体回滚本 PR，但不得恢复双 owner。

### PR-M06-04：机械迁移 Flush 与 Group Commit

- [x] 入口：`[TEST]` M02 的 `try_flush`、legacy adapter 和 SyncFlush/ack 契约测试全部通过；任何行为缺陷先回 M02 修复。
- [x] `[DEV]` 只迁移 flush manager、group-commit request/worker 和 checkpoint 接线；canonical `try_flush` 与 R0 `flush() -> i64` adapter 语义保持不变。
- [x] `[DEV]` 所有 SyncFlush/ack 继续只调用 `try_flush`；legacy adapter 留 facade，不作为内部确认入口。
- [x] `[TEST]` focused test：I/O failure、同批 waiter、group-commit batching、watermark 单调性和 crash/restart。
- [x] `[REV]` 以机械迁移 diff 审查，没有顺带调整 batch 阈值、fsync 策略、错误分类或默认配置。
- [x] 回滚点：flush delegation 指回迁移前模块；不得回滚 M02 正确性契约或恢复 `i64` ack 判定。

### PR-M06-05：迁移 CQ 与 Index

- [x] 入口：`[ARCH]` 20B CQ、Index header/slot、offset 与 Java compatibility golden 已签名。
- [x] `[DEV]` 机械迁移 consume_queue/queue/index 和必要 message encoder adapter，保持 dispatch 顺序和文件路径。
- [x] `[TEST]` focused test：20B unit golden、min/max offset、replay、index query、dirty tail和边界 offset。
- [x] `[REV]` 未引入新的分配/I/O 优化；性能改动留 M10；`get_slice(position,size)` 合同修复有独立回归和证据。
- [x] 回滚点：CQ/Index factory 分别指回旧实现；CommitLog owner和已写格式不变。

内部迁移切片：

- [x] M06-05a：迁移 canonical 20B CQ record codec 与边界校验到 Local。
- [x] M06-05b：迁移 SingleConsumeQueue scan/search/recovery kernel 到 Local。
- [x] M06-05c：迁移 BatchConsumeQueue 与 CQExt storage kernel 到 Local。
- [x] M06-05d：迁移 ConsumeQueue root/store/dispatch owner，Store 保留 composition adapter。
- [x] M06-05e：迁移 40B IndexHeader 与 20B index entry/slot codec 到 Local。
- [x] M06-05f：迁移 IndexFile put/query driver 到 Local。
- [x] M06-05g：迁移 IndexService lifecycle/query/dispatch root，冻结 ledger 并完成父项验收。

### PR-M06-06：迁移 HA、Replication 与 Transfer

- [x] 入口：`[ARCH]` replication capability、ack 条件、leader/follower recovery 和 transfer wire 语义已冻结。
- [x] `[DEV]` 机械迁移 HA、replication、transfer 和相关 checkpoint adapter；background work 继续由 ServiceContext/TaskGroup 拥有。
- [x] `[TEST]` focused test：HA handshake/offset、replica catch-up、leader restart、transfer partial write和shutdown deadline。
- [x] `[REV]` 检查没有第二 WAL、没有 detached thread/task，HA 状态不泄漏到 store-api。
- [x] 回滚点：composition 选择旧 HA/transfer adapter；不回滚 CommitLog/flush owner或持久数据。

### PR-M06-07：迁移 Timer、POP 与 Local Services

- [x] 入口：`[TEST]` Timer/POP/revive/cold-data/stats 当前行为和 feature fixture可重复。
- [x] `[DEV]` 分批机械迁移 timer、pop、services、stats、hook/filter adapter；每批使用独立提交。
- [x] `[TEST]` focused test：timer recovery/expiry、POP checkpoint/revive、cold-data check、service start/stop和feature gates。
- [x] `[REV]` 检查 owner/task/budget 不变，未把 Broker 私有状态或 façade 依赖带入 Local。
- [x] 回滚点：按 Timer、POP、services 三个 delegation 独立切回；已迁其他模块不受影响。

### PR-M06-08：LocalFileMessageStore Facade、Composition 与 Config

- [x] 入口：`[ARCH]` Local 子模块已分别通过 focused test，公开 `LocalFileMessageStore`、Serde/default/alias 基线已冻结。
- [x] `[DEV]` 保留 public type/facade，内部按 lifecycle/query/reput/cleanup 组合已迁模块；config 分为旧 Serde envelope 与 normalized backend config。
- [x] `[DEV]` 仅做 composition 和机械拆分；任何 lifecycle/query 行为修复进入独立 PR 并先加回归测试。
- [x] `[TEST]` focused test：public-path compile、config round-trip/default、load/start/shutdown/destroy、query/reput/cleanup。
- [x] `[REV]` 检查约 500 行审查信号与约 800 行上限、模块单向依赖、facade 无算法回流。
- [x] 回滚点：LocalFileMessageStore composition 指回上一组合实现，旧 config envelope 与数据目录保持。

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

## M06-03ab MappedFile raw byte/progress owner extraction evidence

- [x] `[DEV]` `rocketmq-store-local::mapped_file::MappedFileRawCore` 现为 `MappedFileProgress` 与 mapped-file 原始字节/
  范围算法的唯一 canonical owner，并由 `mapped_file` 根精确导出。Local core 不持有 mmap、文件、`ArcMut`、平台指针或
  Store 类型；调用方通过 `FnOnce` provider 延迟借入 `&[u8]`/`&mut [u8]`。它统一拥有 file/full/read/write/commit/flush progress、normal/
  transient readable position、copied/checked read、append update/no-update、direct range/commit、segment/indexed write、
  raw/readable slice、append/flush/timestamp 记录以及 lock/cache 校验委托。
- [x] `[COMPAT/SEMANTICS]` `DefaultMappedFile` 的 `progress: MappedFileProgress` 已替换为唯一
  `raw_core: MappedFileRawCore`；原始读写方法把延迟 mmap provider 委托 Local，Local 先按旧顺序校验范围、再 materialize
  mapping；有效 target 但无效 source 仍会先取得 mapping。provider 返回短 slice 时以 `None`/`false` fail-closed，不由安全
  公开 API 索引 panic。callback append 的消息编排与 metrics
  仍在 Store，成功结果继续由 core 记录 append progress/timestamp。迁移机械保留原 `+`、cast 与比较表达式，不引入
  `checked_add` 或 overflow 行为修复；`get_slice` exact-end 仍拒绝，`put_slice` exact-end 仍允许，零长度规则与
  `data.len() == length` 优先分支不变。公开操作以 update/no-update、normal/transient 等命名区分，不新增 positional bool。
  `flush_range` 保持“范围校验→开始计时→transient committed 更新→mmap flush→metrics”顺序。
- [x] `[TEST]` TDD RED 由 `cargo test -p rocketmq-store-local --test mapped_file_raw_core` 的 E0432 证明
  `mapped_file::raw`/`MappedFileRawCore` owner 缺失；初始 GREEN 后 Local raw fixture 9/9、Local default/all-feature 全量和
  `DefaultMappedFile` 30/30 均通过。独立审查的 lazy invalid-request 回归先稳定 RED，再由延迟 provider 修复为 GREEN；
  Local provider/短 slice fixture 增至 10/10，Store `DefaultMappedFile` 增至 31/31。M06 source/mutation contract 新增 Local owner 与 Store adapter 两项，新增/关联
  targeted 4/4 通过。首次完整 117 项运行（363.862s）发现 12 项旧契约仍匹配 Store `progress` 直连；将 owner/
  reference 图与拒绝变异 fixture 精确同步为 `kernel -> raw core -> Store adapter` 后，lock/cache/warmup/progress-policy
  的正向与反向门禁均通过，审查前完整 M06 contract 117/117（446.831s）通过；两项 Important 修复后最终 117/117
  （451.885s）再次通过。
- [x] `[CONTRACT]` contract 锁定唯一 Local struct/字段/import/root re-export、45 个公开命名操作、无 cfg/positional bool/
  mmap/storage/platform owner，以及 copied/checked read、append、direct write、segment/indexed write、raw slice、commit 与
  flush-range 的关键 body。Store contract 锁定唯一 raw-core 字段/初始化、纯 raw 方法的单次委托、选择/zero-copy/
  flush adapter 的必要调用和延迟 provider，并拒绝 eager mmap 求值、短 slice 直接索引、exact-end、progress update、
  `data.len() == length`、direct zero commit、导入别名、cfg、重复 owner、手写 Store raw policy 与 normal/transient 接错等 mutation。
- [x] `[FEATURE/PLATFORM]` 未修改 manifest、feature 或依赖。Local 七组显式 closure 加 all-feature、Store default/
  六组有效 closure 加 all-feature、两 crate all-target/all-feature package Clippy、root exact workspace Clippy 与 Local
  strict Rustdoc 均通过；Store 普通 Rustdoc只复现 4 个未触及的 invalid-HTML warning。裸 Store
  `--no-default-features` 继续精确复现既有空 backend enum 的 124 个 E0004 与 unused `ArcMut`，未标记为通过。
  审查前 Windows 固定隔离 target 通过 Local 9/9、Store 30/30 和两 crate all-feature check，cleanup 删除 11,108 files/
  14,262,496,513 bytes；WSL/Linux 同组验证通过，cleanup 删除 10,928 files/13,240,484,920 bytes，两固定路径最终均不存在。
- [x] `[REV]` architecture baseline+35 tests、ArcMut 63 tests+24 fixtures+final guard、AGENTS routing、workspace fmt、
  Python compile 与 diff 检查均通过。ArcMut 初扫只产生同 identity、同 `impl DefaultMappedFile` item 的 1 NEW/1 STALE；
  按 ADR-013 完成一对一 relocation approval 与 monotonic promotion/compare，ledger 保持 1,232 identities/3,372
  occurrences，无跨 item 新债务。error hygiene 只复现未触及的 Broker source stringification、MCP anyhow 与两份缺失
  治理文档基线。首次独立审查以 Critical/Important/Minor = 0/2/0 要求修复 eager lazy-mmap 与短 slice panic；
  两项均经 RED→GREEN 与 mutation contract 修复，同一审查者对最终快照签署 0/0/0、`Approved`。
- [x] `[SCOPE]` M06-03ab 只迁移 `DefaultMappedFile` raw byte/progress owner；不迁移 select/result/`ArcMut` mapping、
  storage/reference lifecycle、实际 mmap flush、平台 lock/warm/cache 探测、metrics、transient pool、公开 `MappedFile`
  trait、flush/group commit、CQ/Index、HA、Timer/POP、runtime ownership 或持久格式。PR-M06-03 父项、入口/DEV/TEST/REV、
  M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03ac CommitLog append frame kernel extraction evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::append_frame::AppendFrameKernel` 现为 append frame runtime
  字段 finalization、segment-roll 判定与 blank marker 编码的唯一 canonical owner。Local 以命名
  `HostWidth::{Ipv4,Ipv6}` 区分 host 布局，并返回命名 `AppendFrameCrcPlan`；不依赖 Store/common/
  `rocketmq-rust`，不持有 `MessageExt`、config、`DashMap`、`ArcMut`、mapped file 或 CRC32 执行器。
  Store 的标准、batch 与 zero-copy callback 只提供业务计算后的标量和借入字节切片。
- [x] `[COMPAT/SEMANTICS]` 固定布局保持 queue offset `20..28`、physical offset `28..36`、IPv4
  store timestamp `56..64`、IPv6 store timestamp `68..76`；仅 `encoded_len + 8 > max_blank`
  才 roll。blank marker 仍是 `[max_blank big-endian][BLANK_MAGIC_CODE big-endian]` 八字节，EOF
  实际只写八字节但结果 `wrote_bytes = max_blank`。标准与 zero-copy 由 Local 计算完全相同的 CRC
  covered/trailer range，Store 仍在原位置执行 `crc32`/`create_crc32`；batch 保留原
  `if enabled_append_prop_crc { let _check_size = msg_len - crc32_reserved_length; }` no-op 算术。
  prepared/rollback transaction 的 queue offset 归零 gate、message-id、result/context、metrics 与业务配置
  均留在 Store。三个 EOF 分支保持“roll 判定→scratch mutex lock→clear→Local marker 编码→`put_slice`
  →原时点计时/实际 I/O”的次序，marker 不在加锁前构造；普通 `i32` `+`/`-`/cast/panic 行为未改成
  checked、saturating 或 fail-closed 策略。Local safe finalization API 已明确记录短 frame 与旧算术的
  `# Panics` 不变量。
- [x] `[TEST]` TDD RED 由 `cargo test -p rocketmq-store-local --test commit_log_append_frame_kernel`
  的四个 E0432 证明 `commit_log::append_frame` owner 尚不存在；GREEN 后 Local kernel fixture 7/7、
  Store callback focused 2/2、Local default/all-feature 全量和 Store default/all-feature check 均通过。
  M06 contract 在既有 append source/value 两项测试内扩充唯一 owner、adapter、exact-body、dataflow/order
  与 mutation guard，关联 focused 2/2（3.516s）通过并保持完整 suite 为 117 项。主线程首次完整运行
  116/117（496.243s），准确暴露旧 planning canonical 文件集合未登记新增 `append_frame.rs`；最小补齐清单后
  失败项 focused 1/1（2.637s）通过，最终候选快照完整 117/117（508.371s）通过。独立初审以
  Critical/Important/Minor = 0/1/0 指出 Store adapter contract 未精确冻结三条 finalizer 参数、第三条
  zero-copy EOF scratch write 与 EOF/normal 两个分路径 timer；审查者证明“改名第三个 write、normal timer
  后移、standard queue/physical 对调、reserved length 改 0、把两个 timer 都放入 EOF 分支”五个 mutation
  最初均漏杀。收紧 contract 后五项全部被杀死，新增 source/mutation focused 1/1（44.699s）通过；该
  contract-only 修复后的最终候选由主线程完整重跑 117/117（519.061s）通过；原 reviewer 复放五项
  mutation 并额外验证 batch/zero-copy 参数错接后，以 Critical/Important/Minor = 0/0/0 Approved。
- [x] `[CONTRACT]` contract 锁定五个 Local owner 的唯一 production 定义、root module、精确 enum/
  struct/常量/方法签名与 body、`HostWidth` timestamp 映射、strict roll、blank big-endian 次序、CRC
  range 和 batch disabled plan；拒绝 positional bool、Store/common/`ArcMut`/mapped-file/CRC execution
  owner。Store contract 锁定三次 roll、三次“lock→clear→marker→scratch”、两次标准/zero-copy
  finalization与一次 batch finalization的完整 frame slice/queue/physical/timestamp/host/CRC-reserved 参数
  顺序和值、三条 EOF `write_bytes_segment`、zero-copy `first timer < EOF write < EOF return < second timer
  < direct buffer` 的分路径边界、两条 transaction gate、两条 CRC 执行与原 callback/I/O/计时 dataflow，
  并拒绝固定偏移、blank magic、roll 算法或 batch CRC 算术回流。mutation 覆盖 `>`→`>=`、
  offset/timestamp 漂移、blank 字段错接、CRC 条件、Host bool、cfg owner、错误 adapter 参数、缺失委托、
  transaction offset、checked batch subtraction、Store 固定切片与 blank import。
- [x] `[FEATURE/REV]` 未修改 manifest、feature 或依赖。Local/Store 共 12 组有效 feature closure、两
  crate all-target/all-feature package Clippy、root exact workspace Clippy、workspace fmt、Local strict
  Rustdoc 均通过；Store 普通 Rustdoc 只复现四条未触及的 invalid-HTML warning。architecture 35/35、
  fixtures 与 baseline，ArcMut 63/63、24 fixtures 与裸 final guard，以及 AGENTS routing 均通过；ArcMut
  ledger 保持 1,232 identities/3,372 occurrences，未新增 relocation approval 或 baseline 债务。
  error hygiene 只复现未触及的 Broker source stringification 1、MCP anyhow 8 与缺失治理文档 2；裸
  Store `--no-default-features` 只复现既有 124 个 E0004 与一个 unused `ArcMut`，两者均未标记为通过。
- [x] `[SCOPE]` M06-03ac 只迁移 append frame finalization/roll/blank 纯字节 kernel；不迁移或修改实际
  mapped-file I/O、CRC32 算法执行、message/config/topic/transaction/result/context/timing/metrics、flush/
  group commit、CQ/Index、HA、Timer/POP、runtime ownership 或持久格式。PR-M06-03 父项、入口/DEV/
  TEST/REV、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03ad CommitLog append TOTALSIZE and batch traversal extraction evidence

- [x] `[DEV]` `rocketmq-store-local::commit_log::append_frame::AppendFrameKernel::declared_frame_length`
  现为 CommitLog append `TOTALSIZE` offset-zero big-endian 解码的唯一 owner；公开 seam 明确记录短 slice
  的 `# Panics`，并保留 `[0..4]`、`try_into`、`unwrap` 与 signed `i32` 语义。命名
  `AppendBatchFrameCursor` 唯一持有 `total_msg_len/msg_pos/index/msg_num`，命名 `AppendBatchFrame`
  只暴露 declared length、start/end、index、cumulative length 与命名 `physical_offset(wrote_offset)`；后者
  精确保留 `wrote_offset + cumulative as i64 - declared as i64` 的旧左结合和 debug overflow seam。
- [x] `[COMPAT/ORDER]` batch cursor 采用两阶段推进：`next` 只执行 strict `<` gate、Local length decode、
  普通 `i32 +=` cumulative update 与 descriptor 构造；Store 先用 cumulative length 判定 roll，之后才求
  physical offset/end、patch frame、保留 batch CRC no-op、更新 context，最后调用 `finish_frame`，并按旧顺序执行
  `msg_num += 1; msg_pos += declared_len as usize; index += 1`。因此 roll 路径不触发 lazy end/physical
  calculation 或 consumed-state advance；成功结果仍取 cursor 的 cumulative bytes 与 message count。
- [x] `[TEST]` TDD RED 以 1 个 E0432 与 3 个 E0599 证明 Local cursor/decoder seam 尚不存在；GREEN 后
  Local golden/panic/legacy anomaly fixture 14/14 通过，覆盖正负 big-endian prefix、短 prefix panic、两帧
  cumulative/physical offset、zero-length non-progress、malformed tail、malformed rolling frame 的 lazy order，
  以及第一帧 `cumulative == declared` 时 `physical_offset(i64::MAX)` 在旧式第一步 addition 上 debug panic。
  Store callback 既有 focused 2/2 保持通过。未新增 Store runtime batch fixture：直接复用
  `DefaultMappedFile` 会新增 transitive ArcMut wrapper occurrence，而为两条断言引入完整 fake 会扩大高触达 callback
  测试模块与范围；因此 batch success parity、second-frame roll 和 context/finish order 由 Local golden 与精确 Store
  adapter/mutation contract 联合验证。初审修复后关联 owner/adapter/mutation contract 2/2（87.897s）通过；
  修复前候选快照完整 M06 contract 曾为 117/117（527.592s），本次初审修复后的完整 117 项复跑留主线程完成。
- [x] `[CONTRACT]` Local contract 锁定 decoder、descriptor、cursor 字段、签名、精确 body、Default delegation、
  lazy end、左结合 physical offset 与 `next -> Store consume -> finish_frame` 顺序；Store contract 锁定标准/zero-copy
  各一次 Local decoder、唯一 batch cursor、cumulative roll、physical/finalizer/CRC/context/finish 顺序及成功结果来源。
  mutation 拒绝 BE→LE、`[0..4]`→`[1..5]`、`<`→`<=`、checked/saturating cumulative、physical offset
  括号重组、checked/saturating physical arithmetic、finish 内推进重排、Store 直接 `from_be_bytes`/固定 prefix、
  手写 cursor/physical arithmetic、提前/遗漏 finish 与 roll 前求 end。
- [x] `[REVIEW FIX]` 初审 Critical/Important/Minor = 0/1/1。Important 指出“Local 先算 relative delta、Store
  再加 wrote offset”会把旧左结合改成 `wrote + (cumulative - declared)`，从而改变 overflow 行为；现由 Local
  `physical_offset` 单点拥有原表达式，并由 golden、debug-overflow 与 regroup/checked/saturating/Store-handwrite
  mutations 冻结。Minor 指出公开 cursor 文档混淆 decoded cumulative 与 finished consumed state，并把 `msg_num`
  误写为 yielded count；现已明确 `next` 只推进 decoded total，非-roll frame 完整消费且 context 更新后才以最近
  descriptor length 恰好调用一次 `finish_frame`，roll 禁止调用，`msg_num` 只统计 finished frame。修复后 Local
  focused 14/14、最终完整 M06 contract 117/117（554.074s）通过；原 reviewer 复放 regroup/checked/saturating/
  Store-handwrite mutations 后以 Critical/Important/Minor = 0/0/0 Approved。
- [x] `[FEATURE/REV]` 未修改 manifest、feature 或依赖。Local default/all-feature 全量测试与 Store
  default/all-feature check 通过；两 crate all-target/all-feature package Clippy、root exact workspace Clippy、
  workspace fmt、Python compile、diff check 与 Local strict Rustdoc 通过，Store 普通 Rustdoc 只复现四条未触及的
  invalid-HTML warning。architecture 35/35+fixtures+baseline、ArcMut 63/63+24 fixtures+final guard 与 AGENTS
  routing 通过；本切片新增 ArcMut 为 0，未新增 relocation approval 或 baseline 债务。新增 cursor 初次 package
  Clippy 精确暴露 `new_without_default`，以 `Default -> Self::new()` 修复后复跑通过。
- [x] `[SCOPE]` M06-03ad 只迁移 append TOTALSIZE 解码与 batch frame traversal owner；Store 继续持有
  message/config/topic/transaction、CRC 执行/no-op、context、message-id、timer/result 与 MappedFile I/O。
  recovery/header/encoder、MappedFile raw/flush、CQ/Index、HA、Timer/POP、runtime ownership 与持久格式均未修改。
  PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03ae CommitLog fixed-header and timestamp probe extraction evidence

- [x] `[DEV/API]` 新增 `rocketmq-store-local::commit_log::header` 作为 CommitLog fixed-header 唯一 owner，冻结
  magic/sysflag 位置 `4/36`、born/store IPv6 flags `0x10/0x20`、host 长度 `8/20`、Store timestamp
  位置 `56/68` 与 V1/V2 magic。`HostWidth` 类型及 variants 保持公开，以原路径
  `commit_log::append_frame::HostWidth` 精确 re-export；flags 与四个选择/长度方法保持 `pub(crate)`，不为测试扩大
  API。V1/V2 magic 仍由 `commit_log::record` 精确 re-export，兼容既有调用路径。
- [x] `[COMPAT/ORDER]` recovery probe 依次读取 magic `(4,4)`、sysflag `(36,4)` 与按 born-host flag
  选择的 timestamp `(56|68,8)`；只有 provider 返回 `None` 才按零值回退，provider 返回 `Some` 短字段继续
  保留 direct-slice panic。无效 magic 在 sysflag read 前立即返回，零 timestamp 返回 `None`。严格 frame reader
  保留 big-endian 直接索引与短 frame panic；Store recovery 只传
  `|offset, len| mapped_file.get_bytes(offset, len)`，checkpoint safe-index/normal 分支、两处 `<=`、最小时间戳与
  logging 保持 Store 所有。`pickup_store_timestamp` 保留范围、`get_message` 和 `-1` sentinel，并以 fully-qualified
  Local strict reader 读取 buffer。
- [x] `[TEST]` RED replay 临时移除 `pub mod header` 后精确得到 6 个 E0432，证明 append/record/parser 的新 owner
  seam 缺失；恢复模块后 Local header focused 10/10 通过。golden 覆盖 V1 IPv4、V2 IPv6、`0x10/0x20` 独立
  born/store flag、精确 read sequence、missing magic/sysflag/timestamp 零回退、invalid magic short-circuit、zero
  timestamp、provider `Some-short` panic、strict IPv4/IPv6 big-endian 及两个 strict short-frame panic。Local
  default/all-feature 全量测试均通过。未新增 Store runtime wrapper fixture：构造真实 `DefaultMappedFile` 会引入
  transitive ArcMut 测试指纹，而 Local golden 与精确 Store adapter contract 已覆盖本切片的两条 Store 调用路径。
- [x] `[CONTRACT]` dedicated header owner/adapter contract 锁定常量、唯一 owner、窄 visibility、`HostWidth` 四方法、
  probe/strict/helper exact body 与 panic 文档、record/append compatibility re-export、parser host-width delegation、
  recovery closure/checkpoint/logging 及 pickup strict adapter。mutation matrix 拒绝四个 offset、两个 flag、两个 host
  length、V1/V2 magic、BE→LE、read width、missing→非零、zero timestamp 接受、invalid magic 后继续读 sysflag、
  `Some-short` 变 checked fallback、strict checked slice/`Option`、Store 两处 checkpoint 与 pickup 的 `<=`→`<`、
  parser/append 手写布局及 Store/parser/append 重复常量；dedicated 1/1 与受影响既有 planning/append/record 4/4
  均通过，主线程最终完整 M06 contract 118/118 通过（553.420s）。
- [x] `[FEATURE/REV]` 未修改 manifest、feature、依赖、runtime ownership、unsafe 或 error mapping。Store
  default/all-feature check、两 crate all-target/all-feature package Clippy、root exact workspace Clippy、workspace fmt、
  Local strict Rustdoc 与 AGENTS routing 通过；Store 普通 Rustdoc 只复现四条未触及的 invalid-HTML warning。
  architecture 35/35+fixtures+baseline、ArcMut 63/63+24 fixtures+final guard 通过。初次 ArcMut final 因删除 recovery
  旧 magic import 的相邻 token context 产生同 identity 的 1 NEW/1 STALE；现以带原因的最窄 `unused_imports`
  compatibility import 保持既有 `DefaultMappedFile` fingerprint，最终新增 ArcMut 为 0，未新增 relocation approval
  或 baseline 债务。初次 Store package Clippy 的 `useless_asref` 已通过直接传 buffer 修复并复跑通过。独立审查
  Critical/Important/Minor = 0/0/0，另行复跑 header 10/10、record parser 3/3 与 dedicated contract 1/1 通过。
- [x] `[SCOPE]` M06-03ae 只迁移 fixed-header layout、host-width 选择与 timestamp probe/strict read；不迁移或修改
  M06-03ad cursor/TOTALSIZE、append CRC/body encoder、record parser bounded reader/error order、recovery scan loop、
  flush/group commit、CQ/Index、HA、Timer/POP、MappedFile lifecycle/I/O、checkpoint policy、runtime ownership 或持久格式。
  PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03af0 CommitLog EOF retry encoded buffer ownership evidence

- [x] `[DEV/API]` `AppendMessageCallback` trait 现明确 EOF ownership：实现若 `take` message 的 encoded buffer，
  返回 `AppendMessageStatus::EndOfFile` 前必须归还同一个 buffer，供 caller 在下一 CommitLog segment 重试；不得
  clone 或 re-encode。`DefaultAppendMessageCallback` 的 standard、batch、zero-copy 三条 EOF branch 各只新增一条
  `encoded_buff = Some(original_bytes_mut)`，位置严格在 blank-marker write 后、EOF result return 前；成功、error、
  fallback 与其他字段/状态不变。
- [x] `[TEST/RED]` 512B deterministic 端到端 RED 的实际编码尺寸与预分析一致：single 先写 340B seed，170B
  message 因剩余 172B 不满足 `170 + 8` 而 roll；batch 先写 170B seed，两个 170B frame 的第一个已遍历、第二个
  cumulative 340B 因剩余 342B 不满足 `340 + 8` 而 roll。基线 second callback 分别在
  `do_append` 第 153 行和 `do_append_batch` 第 256 行的第二次 `take().unwrap()` panic（2 failed，46.4s）。
  callback-level RED 同时证明 standard/batch/zero-copy EOF 后 buffer 均为 `None`（3 failed，28.0s）。
- [x] `[TEST/GREEN]` 修复后端到端 single 精确得到 `PutOk/wrote_offset=512/wrote_bytes=170/msg_num=1`，batch
  精确得到 `PutOk/wrote_offset=512/wrote_bytes=340/msg_num=2`；两次 public put 各只累计两次 lock acquire，证明
  EOF retry 仍在同一次 put lock 临界区内完成。两条 focused 各 1/1 通过。callback 级复用既有
  `DefaultMappedFile` fixture/fingerprint，在同一测试中分别验证 standard、batch、zero-copy EOF 后 buffer `Some`
  且下一 segment retry `PutOk`，最终 1/1 通过。
- [x] `[CONTRACT]` Store adapter contract 先结构化提取三条 `SegmentAppendDecision::Roll` branch，再逐条校验唯一
  exact assignment、blank-marker write `<` restore `<` EOF return 的 branch-local 顺序，并禁止 restore region 的
  clone/re-encode；不是全文件字符串计数。mutation matrix 对三条路径分别拒绝删除 restore、移到 return 后、写错
  `msg_inner/msg_batch` 字段及 `BytesMut::clone`，并以负向 mutation 证明 trait ownership 文档 guard 可拒绝必需片段
  退化；最终 focused contract 1/1 通过（127.606s），主线程完整 M06 contract 118/118 通过（674.963s）。
- [x] `[FEATURE/REV]` 未修改 manifest、feature、依赖、编码格式、CRC、cursor、queue/context、result 字段、
  MappedFile I/O 或 runtime ownership。Store all-target/all-feature package Clippy、Local default/all-feature 全量测试、
  workspace formatter与 diff check 通过。architecture 35/35+fixtures+baseline、ArcMut 63/63+24 fixtures+final guard、
  AGENTS routing 均通过。初次 callback helper 新增显式 imports/返回类型造成 1 NEW/1 STALE import 与两个 transitive
  `DefaultMappedFile` references，收敛后又剩一个 constructor reference；最终把三场景合并复用原测试函数内唯一既有
  constructor fingerprint，新增 ArcMut occurrence 为 0，未修改 baseline 或 relocation approval。独立审查初次
  Critical/Important/Minor = 0/0/1，仅指出 trait 文档 guard 缺少负向 mutation；补齐并复跑后最终 0/0/0 Approved。
- [x] `[SCOPE]` M06-03af0 只修复 EOF retry encoded buffer 所有权并新增 focused tests/contract；不 clone、
  re-encode 或改变其他 EOF 状态，不迁移 append/recovery owner，不修改 flush/group commit、CQ/Index、HA、Timer/POP、
  MappedFile lifecycle、runtime ownership 或持久格式。PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和
  M06-04..12 保持未完成。

## M06-03af CommitLog bounded append-attempt orchestration evidence

- [x] `[DEV/API]` 新增纯同步
  `rocketmq-store-local::commit_log::append_attempt::CommitLogAppendAttempt::run`，以 initial segment 加四个 closure 接收
  `is_full/acquire/lock_active/append` 操作，只依赖 Local `AppendMessageResult/AppendMessageStatus`。显式 outcome
  区分 `Completed::{PutOk, RetryRejected}` 与
  `Aborted::{InitialSegmentUnavailable, InitialActiveLockFailed, InitialMessageIllegal, InitialUnknown,
  RolledSegmentUnavailable, RolledActiveLockFailed}`；roll 完成结果携带旧 segment，rolled abort 同时保留首次 EOF
  result 与旧 segment，Store 无需从状态码反推控制流。
- [x] `[COMPAT/ORDER]` 编排严格冻结旧顺序：initial `Some` 只检查一次 `is_full`，`None/full` 只 acquire 一次，
  active lock 后 append；只有首次 `EndOfFile` 可再执行一次 acquire、active lock 与 append，retry 的任何非 `PutOk`
  状态均成为 `RetryRejected`，不存在第三次尝试。DropProbe 另行冻结 full initial 的 acquire RHS 先于旧 segment
  drop；Store 两条 rolled abort 先 release put lock、drop topic guard、记录原日志，再显式 drop old 并 return，且不走
  warm unlock。其余 put-lock 计时、observability、慢写 warning、warm unlock、offset/stats、flush/HA 与失败 warning 顺序不变。
- [x] `[DEV/ADAPTER]` Store single/batch 各保留一条 exact closure adapter。为满足 borrow checker，active-memory-lock
  实现机械拆为无 `DefaultMappedFile` 类型参数的 parts helper；single、batch 与既有 test wrapper 共同调用私有
  `lock_active_mapped_file_parts!`，其内部只求值一次 mapped-file expression，并保留全 production 唯一的 mapped-file
  `lock_region_with` adapter。既有 default wrapper 继续一对一委托，未 clone lock、`Arc` 或 `ArcMut`。Local owner
  不包含 `MessageExt*`、`MappedFile*`、Store status enum、Tokio、`Instant` 或 `ArcMut`，也不拥有 mmap/I/O、日志、
  计时及后处理策略。
- [x] `[TEST/RED/GREEN]` RED 首先以缺少 `append_attempt` module 得到 4 个 `E0432` unresolved-import，并以
  Store 尚未接入 seam 得到 9 项 focused contract 违规。GREEN 后 Local event/drop exhaustive fixture 8/8 通过；
  fixture 的 `Segment` 刻意不实现 `Clone`，同时证明 `run` 不要求 `Clone` bound；
  既有 512B af0 single/batch 回归各 1/1，通过结果仍分别为 `PutOk/offset=512/wrote=170/msg_num=1` 与
  `PutOk/offset=512/wrote=340/msg_num=2`。Local default 与 all-feature 全量测试均通过。
- [x] `[CONTRACT]` dedicated contract 固定 Local module/owner/import、outcome shape、generic closure signature、
  acquire/lock/append 上界、事件与 drop 顺序、retry status 全覆盖；同时固定 Store 两条完整 closure call、八类 outcome
  到唯一 `PutMessageStatus` 的逐 arm 映射、rolled abort 生命周期、唯一 macro lock-region owner、active-lock
  parts/wrapper 委托以及 af0 回归。mutation matrix 拒绝 is-full 绕过、
  acquire/drop 逆序、缺锁、错误 segment、第三次 append、retry 状态遗漏、clone/cfg/duplicate、import alias、两条
  adapter 参数/append 变异、batch illegal/unknown status 对调、错误 result 映射、old 提前/遗漏 drop、wrapper 加逻辑/
  clone、错误 API 文档及测试删除；memory-lock focused 3/3、append-attempt focused 2/2 通过。
- [x] `[FEATURE/REV]` 未修改 manifest、feature、依赖、公开 wire/storage 格式或 runtime ownership。Local/Store
  all-target/all-feature package Clippy、workspace fmt/diff、architecture 35/35+fixtures+baseline、ArcMut
  63/63+24 fixtures+final guard 与 AGENTS routing 均通过。active-lock 首版 parts 签名曾产生 1 NEW/2 STALE
  `DefaultMappedFile` 指纹；最终改为 generic lock-region closure 并复用既有三个 wrapper/type-reference 指纹，未修改
  ArcMut baseline 或 relocation approval。主线程首次完整 M06 contract 120 项暴露 2 项契约登记缺口：canonical directory
  尚未登记 `append_attempt.rs`，既有 memory-lock config adapter 尚未冻结新增 parts helper 的 target 类型；补充精确文件集合、
  target 引用计数、parts helper 完整签名与 closure bound，并加入两条类型替换 mutation 后 focused 2/2 通过，最终完整
  M06 contract 120/120 通过（602.201s）。实现与契约修正两轮独立复审均为 Critical/Important/Minor = 0/0/0。
- [x] `[SCOPE]` M06-03af 只迁移 bounded append-attempt 控制流；Store 继续拥有 message encode、MappedFile queue/I/O、
  active memory-lock syscall、topic/put lock、计时/日志/result mapping、warm/offset/stats/flush/HA。未迁移或修改
  append-frame/CRC、recovery、flush/group commit、CQ/Index、HA、Timer/POP、runtime ownership 或持久格式。
  PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03ag CommitLog standard recovery declared-frame read owner extraction evidence

- [x] `[DEV/API]` 在 Local `commit_log::record` 新增纯同步 `read_declared_frame(position, read)`；callback 为
  `FnMut(usize, usize) -> Option<Bytes>`，返回 `(Option<Bytes>, usize)`。Local owner 只解释四字节 big-endian signed
  frame size，不依赖 `MappedFile`、`MessageExt*`、Store、Tokio 或 `ArcMut`。
- [x] `[COMPAT/ORDER]` 冻结原 standard recovery 一次性读取语义：先且仅先读 `(position, 4)`；缺头或非正长度返回
  `(None, 0)` 且不读正文；successful short header 保持 `Buf::get_i32` exact-read 前置条件并按 Rustdoc `# Panics`
  明示 panic；正长度再且仅再读 `(position, declared_size)`，缺正文仍返回 `(None, declared_size)`。不新增 body
  length、format 或 CRC 校验，两次读取使用同一 position。
- [x] `[DEV/ADAPTER]` 删除 Store `get_simple_message_bytes`；standard `recover_normally` 与 `recover_abnormally` 各保留
  一条 `mapped_file.get_bytes(position, size)` closure adapter。`recover_normally_optimized`、
  `recover_abnormally_optimized` 与 `CommitLogFrameCursor` 未修改；既有日志、SourceEnded/InvalidRecord、checked-add、
  dispatch、checkpoint 与 truncate 顺序保持不变。
- [x] `[TEST/RED/GREEN]` RED 先得到缺少 `read_declared_frame` 的 `E0432`。GREEN 后 Local focused 5/5 通过，覆盖
  exact read events/参数、big-endian、缺失/短头、零/负长度、缺正文保留长度与成功读取；Store standard normal/abnormal
  dirty-tail recovery 2/2 通过。Local 与 Store all-target/all-feature package Clippy 均通过。
- [x] `[CONTRACT]` dedicated baseline+mutation 2/2、canonical owner/facade 2/2 通过。契约固定 Local 唯一非 cfg owner、
  exact signature/body/panic 文档、无 Store/runtime 类型，以及 standard normal/abnormal 各一次和 optimized 零次调用；
  mutation matrix 拒绝 header 长度、little-endian、`< 0`、第二次 offset/长度、缺正文 size 归零、short-header
  fail-closed、cfg/duplicate/import alias、Store policy copy、adapter 参数、optimized 接入及回归测试删除。
- [x] `[FEATURE/REV]` 未修改 manifest、feature、依赖、baseline、relocation approval、公开 wire/storage 格式或 runtime
  ownership。architecture 35/35+fixtures+baseline、ArcMut 63/63+24 fixtures+final guard 与 AGENTS routing 均通过；
  ArcMut NEW/STALE 为 0/0。workspace fmt 与 diff check 通过；主线程完整 M06 contract 122/122 通过（608.436s）。
  独立复审 Critical/Important/Minor = 0/0/0，确认两条 standard adapter 的 recovery 顺序与 optimized 零调用不变。
- [x] `[SCOPE]` M06-03ag 只迁移 standard recovery declared-frame read owner；不迁移或修改 optimized cursor、record
  parser、CRC/body validation、append、flush/group commit、CQ/Index、HA、Timer/POP、MappedFile lifecycle、runtime
  ownership 或持久格式。PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03ah CommitLog normal recovery segment orchestration extraction evidence

- [x] `[DEV/API]` 新增纯同步、runtime-neutral 的
  `rocketmq-store-local::commit_log::normal_recovery`，由 `NormalRecoveryRecord<R>`、
  `NormalRecoveryObservation`、`NormalRecoverySegmentOutcome<E>` 与
  `NormalRecoveryState::drive_segment` 唯一拥有 normal recovery 单 segment record-loop 编排。driver 通过泛型
  `Next/Started/Observe` closure 接收 adapter，不引入 `dyn`、`Clone`、`Send/Sync/'static`、`ArcMut`、
  `MappedFile`、Store 类型或 `unsafe`。
- [x] `[COMPAT/ORDER]` 冻结原 normal recovery 顺序和所有权：segment state transition 先于 started hook；Message
  state transition 先于 observe；Blank/Invalid observe 先于 state transition；SourceEnded 不触发 observe；payload
  由 driver 持有并在 observe 返回或 state rejection 后释放。adapter error 保留原错误身份，state error 独立返回，
  Stop/ContinueNext 后不再读取下一条 record。
- [x] `[DEV/ADAPTER]` Store `recover_normally_optimized` 与 `recover_normally` 各且仅各调用一次
  `drive_segment`，不再直接调用 normal recovery `apply`。optimized 保留 `BatchMessageIterator`、
  `RecoveryContext::process_message`、成功 message 后 `file_processed`、ContinueNext 后 `files_processed/index`
  更新；standard 保留 `read_declared_frame`、checked cursor advance 与 parser。两条 abnormal recovery 路径不接入
  该 driver；dispatch、warning、summary、checkpoint 与 truncate 顺序不变。
- [x] `[TEST]` Local orchestration focused 7/7、Local all-feature test suite 与 Store CommitLog recovery 19/19 通过。
  覆盖 segment-start state failure、started/next 顺序、adapter error identity、standard/optimized SourceEnded 分歧、
  Message/Blank/Invalid observe/drop 顺序、offset overflow fail-closed、bounded next calls、跨 segment 水位、dirty-tail
  truncate、controller/dup gate 与 RocksDB restart parity。
- [x] `[CONTRACT]` dedicated baseline+mutation 6/6 与完整 M06 contract 124/124 通过（635.252s）。契约固定三个 enum、
  `drive_segment` exact signature/body/closure bounds、唯一非 cfg owner、无动态/Store/runtime 边界、两条 normal Store
  adapter 各一次调用、abnormal 零调用、policy construction、空文件短路、adapter error、summary/final write dataflow、
  原 adapter 统计/日志顺序及七个 regression test。mutation matrix 拒绝 variant/field、transition/observe 顺序、
  SourceEnded observe、error/outcome、closure bound、Clone/dyn、cfg/import alias、Store 直接 state policy、调用次数、
  abnormal 接入、adapter/summary 顺序、空文件旁路、错误继续及 regression test 删除。
- [x] `[FEATURE/REV]` 未修改 manifest、feature、依赖、baseline、relocation approval、runtime ownership 或公开
  wire/storage 格式。Local 与 Store package Clippy、workspace fmt、workspace all-target/all-feature Clippy、architecture
  35/35+baseline、ArcMut 63/63+final guard、AGENTS routing、Python compile 与 diff check 均通过；ArcMut 未新增。
  独立复审确认生产语义无 Critical/Important/Minor 问题，并将 summary、empty-file 与 adapter-error 三项结构缺口
  补入 mutation contract。
- [x] `[SCOPE]` M06-03ah 只迁移 normal recovery 单 segment record-loop 编排；不迁移实际 MappedFile I/O、record
  parser、abnormal recovery 编排、CommitLog 根 owner、完整 append/load、flush/group commit、CQ/Index、HA、Timer/POP、
  runtime ownership 或持久格式。PR-M06-03 父项、入口/DEV/TEST/REV、M06 Exit Checklist 和 M06-04..12 保持未完成。

## M06-03 MappedFile canonical owner extraction evidence

- [x] `[DEV/API]` `rocketmq-store-local::mapped_file` 现为 `MappedFile` trait、泛型
  `DefaultMappedFile<M>`、unsafe `MappedMemory` backend contract、`SelectMappedBufferResult<M>`、reference
  lifecycle、mapping/raw/file kernel 与平台 FFI 的 canonical owner。Store 原公开路径以精确 re-export、
  `DefaultMappedFile<StoreMappedMemory>` type alias 和 `MappedFileAppend` 业务 append extension 保持兼容；
  `CommitLogLoadAdapter` 的 recovery mapping 已收窄为中立 `&[u8]`，Local 不再依赖 Store mapping 类型。
- [x] `[COMPAT/SAFETY]` Store compatibility backend 由 `Arc<MmapMut>` 保持 mapping/zero-copy region 生命周期，
  mapped-file owner 不再持有 `ArcMut`。删除安全的 whole-map `get_mapped_file_mut(&self) -> &mut [u8]` escape，
  内部写路径改用带完整 `# Safety` 契约的 `mapped_file_mut_parts`；select result 的 mutable slice 同时要求
  `unsafe` 调用和 `&mut self`。旧 Store module path、append result/status、compaction 显式拒绝、lazy mmap、
  flush/progress、warmup、memory lock、cache residency 与 zero-copy 行为保持原 adapter 语义。
- [x] `[TEST/CONTRACT]` `cargo test -p rocketmq-store-local --all-features` 全部通过（crate 单测 99/99，
  所有 integration suite 通过），其中新的 DefaultMappedFile owner focused 30/30；Store recovery compatibility
  2/2 与 compaction adapter 1/1 通过。新增 mapped-file owner/facade/backend leak 契约，并同步更新 raw、mapping、
  cache、warmup、memory-lock、loader 与 FFI mutation matrix；最终完整 M06 contract 126/126 通过
  （632.603s）。Windows prefetch 错误保留 typed `io::Error` source，不再 stringification。
- [x] `[ARC/ARCH]` ArcMut ledger 从 1,232 identities/3,372 occurrences 降至
  1,171 identities/3,233 occurrences；`current_milestone` 保持 M05，未新增 relocation approval 或 baseline
  债务。ArcMut final guard、architecture dependency baseline、AGENTS routing、workspace fmt、两 crate package
  check/Clippy、root 28-package all-target/all-feature Clippy、Local strict Rustdoc、Python compile 与 diff check
  均通过。error architecture guard 的本切片 source-stringification 回归已清零，仅复现未触及的 Broker 1 项、
  MCP anyhow 8 项与缺失治理文档 2 项，未将其误记为通过。
- [x] `[INVENTORY/SCOPE]` 总 checklist 已按 82 个顶层 PR 工作包复核：30 已完成、M06-03 进行中、
  51 未开始，合计 52 个尚未完成；root workspace 为 28/32，尚缺 store-rocksdb 与 proxy-core/cluster/local
  四个目标 crate。本切片不关闭 PR-M06-03：CommitLog 根 append/load/recovery owner 与 facade 仍需收口；
  M06-04..12 的 flush/group commit、CQ/Index、HA、Timer/POP、RocksDB 与 Store facade，以及 M07..M12 均未勾选。

## M06-03 native mmap and CommitLog loader owner extraction evidence

- [x] `[DEV/API]` Local `mapped_file` 现直接拥有 `NativeMappedMemory` 与 `MmapRegionSlice`；
  `DefaultMappedFile`、`SelectMappedBufferResult` 的默认 backend 均指向 native owner。Local
  `CommitLogLoader::load_optimized` 直接创建 native mapping，泛型测试/扩展入口收敛为 `load_with_adapter`，
  native open、lazy-read-only 历史文件策略与 fully-loaded position adapter 不再由 Store 构造。
- [x] `[COMPAT/OWNER]` Store `mapped_file::memory` 仅精确 re-export native backend/region，原
  `StoreMappedMemory` 名称通过明确 alias 保持；Store `DefaultMappedFile` 与 select result 继续以专用 type alias
  保持既有类型推导。Store `commit_log_loader` production 区只保留 `LoadStatistics`、
  `RecoveryFilePrefetch`、`RecoveryMmapAdvice`、`CommitLogLoader` 四条精确 re-export，原 wrapper、函数表 adapter、
  文件发现、mapping 创建与 memory-hint 编排均已删除。
- [x] `[TEST]` Local fake/native loader integration 4/4、Store loader compatibility 16/16、Local
  DefaultMappedFile focused 30/30 与 `cargo test -p rocketmq-store-local --all-features` 全部通过；
  `cargo check`、all-target/all-feature package Clippy（Local/Store）、workspace fmt、root workspace
  all-target/all-feature Clippy及 Local strict Rustdoc 均通过。
- [x] `[CONTRACT]` mapped-file/native owner、Store exact-facade、loader native adapter 与 alias-bypass mutation
  契约已同步；修正测试从已删除 Store wrapper 转向精确 facade 所有权验证。最终完整 M06 contract
  126/126 通过（603.347s）；合法 `NativeMappedMemory as StoreMappedMemory` facade 与 crate/module/glob alias
  绕过检测均由独立 mutation 保持 fail-closed。
- [x] `[ARC/ARCH]` 本切片未新增 `ArcMut` identity/occurrence 或 baseline/relocation approval；ArcMut final
  guard、architecture dependency baseline 与 AGENTS routing 均通过。error architecture guard 仍仅复现未触及的
  Broker source-stringification 1 项、MCP anyhow 8 项和治理文档缺失 2 项，未将非零基线误记为通过。
- [x] `[INVENTORY/SCOPE]` 本切片关闭 native mmap/load owner 内部工作，但不关闭顶层 PR-M06-03；
  CommitLog 根 append/load/recovery owner、Store facade 最终收口仍待完成。82 个顶层工作包统计保持
  30 已完成、1 进行中、51 未开始，即 52 个尚未完成；M06-04..12 与 M07..M12 状态不变。

## M06-03ai abnormal recovery segment orchestration extraction evidence

- [x] `[DEV/API]` Local 新增 runtime-neutral `commit_log::abnormal_recovery`，由
  `AbnormalRecoveryRecord<R>`、`AbnormalRecoveryObservation`、`AbnormalRecoverySegmentOutcome<E>` 与
  `AbnormalRecoveryState::drive_abnormal_segment` 唯一拥有 abnormal recovery 单 segment record-loop。
  driver 通过泛型 next/started/observe closure 接收 adapter，不引入 Store、MappedFile、消息类型、runtime、
  `dyn`、`ArcMut` 或 `unsafe` 边界。
- [x] `[ORDER/FAIL-CLOSED]` segment-start 与 message/blank 状态转换先于 observation；invalid warning observation
  先于状态转换；source-end 不触发 observation。dispatch/skip、file-end hook、standard stop 与 optimized next
  保持策略差异；adapter error 保留原错误身份，state error 和 unexpected action 以 typed outcome 返回，生产路径
  不使用 panic/unreachable。payload 在 observation 返回或 state rejection 后由 driver 释放，不要求 `Clone`。
- [x] `[DEV/ADAPTER]` Store `recover_abnormally` 与 `recover_abnormally_optimized` 各且仅各调用一次
  `drive_abnormal_segment`，不再直接 `apply` `SegmentStarted/MessageAccepted/Blank/InvalidRecord/SourceEnded`。
  Store 仅保留 declared-frame/batch 读取、record parser、confirm gate 输入、dispatch/file-end hook、warning、
  optimized file statistics 与最终 checkpoint/CQ truncate/MappedFile 水位写回；两条路径仍使用原 raw input size
  计算 confirm candidate，且 dispatch 与 skip 都计入 optimized 已处理文件。
- [x] `[TEST/CONTRACT]` Local abnormal orchestration 6/6、Store recovery integration 19/19、Store CommitLog
  filtered lib tests 35/35 与 Local all-feature 全量均通过。新增 owner/order/payload/error/adapter mutation contract，
  并将原 Store direct-event contract 收敛为 exact Local driver seam；最终完整 M06 contract 128/128 通过
  （591.305s）。
- [x] `[FEATURE/ARCH]` Local/Store all-target/all-feature package Clippy、workspace fmt/diff check、ArcMut final
  guard、architecture dependency baseline 与 AGENTS routing 均通过；未修改 manifest、feature、持久格式、
  runtime ownership、ArcMut baseline 或 relocation approval。error architecture guard 仅复现未触及的 Broker
  source-stringification 1 项、MCP anyhow 8 项和治理文档缺失 2 项，未将非零基线误记为通过。
- [x] `[INVENTORY/SCOPE]` M06-03ai 关闭 abnormal 单 segment 编排内部工作，但顶层 PR-M06-03 仍未关闭：
  CommitLog 根结构、外层 append/recovery composition 与 Store facade 仍需最终收口。顶层统计保持 82 个工作包中
  30 已完成、1 进行中、51 未开始，即 52 个尚未完成；M06-04..12 与 M07..M12 状态不变。

## M06-03aj CommitLog load outer orchestration extraction evidence

- [x] `[DEV/API]` Local 新增 runtime-neutral `commit_log::load_orchestration`，由
  `CommitLogLoadStep`、`CommitLogLoadObservation<E>`、`safe_load_requested` 与
  `drive_commit_log_load` 唯一拥有 CommitLog safe/optimized/sequential 外层决策。公开 seam 使用泛型
  execute/observe closure，不引入 Store、MappedFile、config、runtime、`dyn`、`ArcMut` 或 `unsafe` 边界。
- [x] `[COMPAT/ORDER]` `ROCKETMQ_SAFE_LOAD` 继续只接受 `1` 或大小写不敏感的 `true`，不 trim 或扩展
  truth value。forced-safe 只执行 sequential；optimized `Ok(true)` 成功，`Ok(false)` 终止且不 fallback；
  只有 optimized adapter error 先 observation、后执行 sequential fallback。sequential adapter error 以 typed
  observation fail-closed 为 `false`，生产路径不 panic/unwrap/expect，也不要求 error `Clone`。
- [x] `[DEV/ADAPTER]` Store `CommitLog::load` 只读取环境值，并把 `load_optimized` 与 `load_sequential`
  映射为两个 exact step adapter；safe/optimized/rejected/fallback legacy 日志保持原文。optimized 成功路径在
  统计日志后、返回 `Ok(true)` 前执行一次 `mapped_file_queue.check_self()`；sequential 的 load/check/log 顺序不变。
- [x] `[TEST/CONTRACT]` Local 新增 6 项 truth-table、terminal outcome、fallback observation order 与 adapter-error
  回归测试；Local all-feature 全量、Store CommitLog filtered lib 35/35、Store recovery integration 19/19 均通过。
  M06 contract 新增 owner/signature/order/import/legacy-log/adapter/test mutation 约束，并修正一条旧 abnormal mutation
  对 rustfmt 换行敏感的夹具；最终完整 contract 130/130 通过（599.364s）。
- [x] `[QUALITY/ARCH]` `cargo fmt --all -- --check`、Local/Store all-target/all-feature package Clippy、
  `git diff --check`、ArcMut final guard、architecture dependency baseline 与 AGENTS routing 均通过；未修改 manifest、
  feature、runtime ownership、ArcMut baseline、持久格式或 relocation approval。error architecture guard 仍只复现
  未触及的 Broker source-stringification 1 项、MCP anyhow 8 项与治理文档缺失 2 项，未将非零基线误记为通过。
- [x] `[INVENTORY/SCOPE]` M06-03aj 关闭 CommitLog load 外层策略内部工作，但不关闭顶层 PR-M06-03：
  CommitLog 根结构、外层 append/recovery composition 与 Store facade 仍需最终收口。顶层统计保持 82 个工作包中
  30 已完成、1 进行中、51 未开始，即 52 个尚未完成；M06-04..12 与 M07..M12 状态不变。

## M06-03ak CommitLog recovery route extraction evidence

- [x] `[DEV/API]` Local 新增 runtime-neutral `commit_log::recovery_orchestration`，由
  `CommitLogRecoveryStep`、`optimized_recovery_requested` 与 `drive_commit_log_recovery` 唯一拥有
  optimized/standard route 决策。同步泛型 driver 只调用一次 `FnOnce` 并原样返回 adapter output，不引入
  Store、MappedFile、config、Future、async runtime、`dyn`、`ArcMut` 或 `unsafe` 边界。
- [x] `[COMPAT]` `ROCKETMQ_USE_OPTIMIZED_RECOVERY` 保留原 Rust `bool` parse 语义：缺失、malformed、大小写不符
  或带空格时默认 optimized，只有精确 `false` 选择 standard。normal/abnormal 缺失 self-reference 时继续使用原
  `skip ... recovery: {error}` 日志并提前返回，公开 async 方法签名不变。
- [x] `[DEV/ADAPTER]` Store `recover_normally` 与 `recover_abnormally` 各且仅各调用一次 Local driver；每条路径
  只保留 optimized/standard 两个 async CommitLog adapter，不再直接解析 bool 或自行执行 `if use_optimized`。
  四个 adapter 继续接收相同 consume-queue physical offset 与同一个 message-store reference。
- [x] `[TEST/CONTRACT]` Local 新增 3 项 truth-table、exact-once route 与 output identity 测试；Local all-feature
  全量、LocalFileMessageStore filtered lib 83/83、Store recovery integration 19/19 均通过。M06 contract 新增
  owner/signature/truth-table/import/四 adapter/test mutation 约束；最终完整 contract 132/132 通过（598.898s），
  import 位置调整后 3 项定向契约再次通过。
- [x] `[QUALITY/ARCH]` `cargo fmt --all -- --check`、Local/Store all-target/all-feature package Clippy、
  ArcMut final guard、architecture dependency baseline 与 AGENTS routing 均通过。新增 imports 曾改变既有 ArcMut
  context fingerprint，恢复原邻接后 guard 重新通过，未修改 baseline/approval。error architecture guard 仍只复现
  未触及的 Broker source-stringification 1 项、MCP anyhow 8 项与治理文档缺失 2 项。
- [x] `[INVENTORY/SCOPE]` M06-03ak 关闭 optimized/standard recovery route 内部工作，但顶层 PR-M06-03 仍未关闭：
  CommitLog 根结构、append/recovery 方法 owner 与 Store facade 仍需最终收口。顶层统计保持 82 个工作包中
  30 已完成、1 进行中、51 未开始，即 52 个尚未完成；M06-04..12 与 M07..M12 状态不变。

## M06-03al CommitLog runtime state owner extraction evidence

- [x] `[DEV/API]` Local 新增 `commit_log::runtime_state`，由
  `CommitLogPutMessageLockRuntimeInfo`、hidden `CommitLogPutMessageLockStats` 与 hidden
  `CommitLogActiveMemoryLock` 唯一拥有 put-message lock 计数/快照和 active memory-lock 当前 handle、文件、范围及
  manager 状态。模块只依赖原子类型、Local memory-lock manager/handle/target，不含 Store、MappedFile、config、
  runtime、`dyn`、`ArcMut`、tracing 或 `unsafe`。
- [x] `[COMPAT/STATE]` put lock 继续用 Relaxed 原子累计 acquire/wait/hold total，并以
  `compare_exchange_weak` 保持 wait/hold max；Store 旧 `CommitLogPutMessageLockRuntimeInfo` 路径是 canonical Local
  类型的精确 re-export。active-window 继续复用同文件内 `[offset, offset + len)`，active-file 继续要求类别、文件、
  offset、len 全部精确匹配；handle 仍先 take、经 canonical manager unlock、再 clear。
- [x] `[DEV/ADAPTER]` Store 删除两组状态 struct/impl，只持有 Local stats 与 active-lock state。Store 保留
  MessageStoreConfig→target、MappedFile address/range、平台 lock/unlock、fast-path presence AtomicBool 与错误传播；
  manager/handle 通过 Local accessor/take seam 使用，不复制状态算法。
- [x] `[TEST/CONTRACT]` Local 新增 3 项 totals/maxima、active-window 半开范围、active-file exact identity 与
  take/unlock/clear 回归测试；Local all-feature 全量和 Store CommitLog filtered lib 35/35 通过。M06 contract 新增
  canonical fields/atomics/order/import/re-export/Store adapter/test mutation，并同步 hidden memory-lock seam caller ledger；
  首次全量仅暴露 2 项 ledger 缺口，修正后定向 4/4，最终完整 contract 134/134 通过（595.846s）。
- [x] `[ARC/QUALITY]` Local/Store all-target/all-feature package Clippy、workspace fmt、dependency baseline 与
  AGENTS routing 均通过。删除 Store stats owner 改变同一 `CommitLog.mapped_file_queue` ArcMut 字段的前置 fingerprint；
  按 ADR-013 完成 `ad39677711ebd03cb15aa7bc`→`982715d2703b3ca7eb658106` 一对一 relocation、promotion、
  baseline compare 与 final guard，ledger 保持 1,171 identities/3,233 occurrences，无新增 ArcMut 债务。
  error architecture guard 仍只复现未触及的 Broker source-stringification 1 项、MCP anyhow 8 项与治理文档缺失 2 项。
- [x] `[INVENTORY/SCOPE]` M06-03al 关闭两组 CommitLog runtime-neutral 状态 owner，但顶层 PR-M06-03 仍未关闭：
  CommitLog 根 composition、MappedFileQueue、append/recovery 方法 owner 与 Store facade 仍需按后续依赖收口。
  顶层统计保持 82 个工作包中 30 已完成、1 进行中、51 未开始，即 52 个尚未完成；M06-04..12 与 M07..M12
  状态不变。

## M06-03am CommitLog composite runtime state owner extraction evidence

- [x] `[DEV/OWNER]` Local `commit_log::runtime_state::CommitLogRuntimeState` 成为 CommitLog 实现运行态的唯一组合
  owner，持有 confirm offset、put-message lock statistics、begin-time-in-lock、active memory-lock/presence 与
  last-load statistics；初始化值、原子 ordering、锁预算和 warn-only 配置均保持既有语义。
- [x] `[DEV/ADAPTER]` Store `CommitLog` 删除六个分散状态字段，只保留一个 `runtime_state` 字段；load、append、
  recovery、controller confirm 与 memory-lock 生命周期通过 Local accessor 委托，Store 继续持有 MappedFile、
  平台 lock/unlock、日志、dispatch、flush/HA/CQ/Index adapter。
- [x] `[TEST]` `cargo test -p rocketmq-store-local --all-features` 全量通过；新增 composite runtime-state 更新测试后
  focused 文件为 4/4。`cargo test -p rocketmq-store --lib log_file::commit_log` 35/35、
  `cargo test -p rocketmq-store --test commitlog_recovery_tests` 19/19 通过。
- [x] `[CONTRACT]` runtime-state/append/memory-lock 定向 baseline+mutation 契约通过；完整 M06 contract 134/134
  通过（614.167s），冻结四个 Local 状态 owner、组合字段/方法、Store direct import/re-export、单 owner、精确
  adapter dataflow 与四个 Local focused test，旧 Store 状态 owner、别名/brace/glob import、clone/cfg/重复定义均
  fail closed。
- [x] `[REV/ARCMUT]` 组合字段替换仅使同一 `struct CommitLog` 内三个既有 occurrence 发生一对一 fingerprint
  变化：identity `250761360f210e16dd1bc14b` 的 `9ed2e1e104c45cd80f56de96`→
  `ff32c4ce45d7a75051d09d81`，identity `b2ef1bf0618590f61e43b12d` 的
  `f562f845a1643d76c37d2cae`→`7e991426dfa471f3448fdf1f` 与
  `abb1aa80c74cf115fedf9d18`→`c374ebb529470db81585545f`。ADR-013 精确 approval、promotion、compare 与更新
  ledger 后裸守卫均通过，债务保持 1,171 identities / 3,233 occurrences。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt check、architecture dependency guard、AGENTS
  routing check 均通过。error architecture guard 仍只报告既有 Broker source-stringification 1 项、MCP anyhow 8 项
  与两份缺失 governance 文档，本切片未新增命中。
- [x] `[INVENTORY/SCOPE]` M06-03am 关闭 CommitLog 组合运行态 owner 内部工作，但不关闭顶层 PR-M06-03：
  CommitLog 根结构、MappedFileQueue、append/recovery 方法 owner 与 Store facade 仍需收口；顶层统计保持 82 个
  工作包中 30 已完成、1 进行中、51 未开始，即 52 个尚未完成；M06-04..12 与 M07..M12 状态不变。

## M06-03an MappedFileQueue runtime progress state owner extraction evidence

- [x] `[DEV/OWNER]` Local `mapped_file::queue_state::MappedFileQueueRuntimeState` 成为 mapped-file queue 的
  flushed/committed physical offsets、full-flush store timestamp 与 commit serialization lock 唯一 owner；继续使用
  `Arc<AtomicU64>`/`Arc<Mutex<()>>`，保留 Acquire、Release、SeqCst ordering 和 signed offset 按位往返语义。
- [x] `[DEV/ADAPTER]` Store `MappedFileQueue` 删除四个分散状态字段，只持一个 `runtime_state`；default/new、
  commit、get/set committed/flushed/store-timestamp 均精确委托 Local。Store 继续拥有 store path、mapped-file size/
  collection、AllocateMappedFileService 与 load/create/delete/find/flush/CQ timestamp I/O 编排。
- [x] `[TEST]` Local 新增 initial/signed-offset 与 commit-lock serialization 2/2；
  `cargo test -p rocketmq-store-local --all-features` 全量通过，`cargo test -p rocketmq-store --lib` 535/535 通过，
  覆盖 mapped-file queue、CommitLog、flush、CQ、HA、Timer 等共享消费者。
- [x] `[CONTRACT]` 新增 baseline+8 类 mutation 两项契约，冻结 Local 四字段顺序/默认值/ordering/lock、唯一 module/
  definition、Store direct import、单 owner、两次构造和七条精确 adapter，并拒绝旧 Store 字段、alias、bypass 与测试
  删除。首次完整运行只暴露 canonical 文件集合漏列 `queue_state.rs`，补齐后定向 3/3、最终 M06 contract 136/136
  通过（610.280s）。
- [x] `[REV/ARCMUT]` `python scripts/arc_mut_guard.py` 直接通过，无 identity/occurrence/fingerprint 变化，ledger
  保持 1,171 identities / 3,233 occurrences，无需 ADR-013 relocation approval 或 baseline 更新。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt check、architecture dependency guard 与 AGENTS
  routing check 通过。error architecture guard 仍只复现既有 Broker source-stringification 1 项、MCP anyhow 8 项
  与两份缺失 governance 文档，本切片未新增命中。
- [x] `[INVENTORY/SCOPE]` M06-03an 关闭 MappedFileQueue runtime-neutral progress/commit-lock owner，但不迁移
  `try_flush`/`FlushProgress` 决策，不跨越 M06-04 flush/group-commit 边界。顶层 PR-M06-03 仍需收口 CommitLog 根结构、
  MappedFileQueue I/O/collection、append/recovery 方法 owner 与 Store facade；82 个顶层工作包仍为 30 已完成、
  1 进行中、51 未开始，即 52 个尚未完成。

## M06-03ao MappedFileQueue storage identity owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增无依赖泛型 `mapped_file::queue_storage::MappedFileQueueStorage<T>`，唯一持有
  store path、segment size 与 mapped-file collection；Local 不依赖 ArcSwap、DefaultMappedFile、Store/Common、runtime
  或 tracing，Store 继续选择 `ArcSwap<Vec<Arc<DefaultMappedFile>>>` 作为后端类型参数。
- [x] `[DEV/ADAPTER]` Store `MappedFileQueue` 删除三个直接字段，只持 Local `storage`、Store
  `AllocateMappedFileService` 与 Local `runtime_state` 三个根 owner。default/new 注入 path/size/ArcSwap；load/create/
  delete/find/flush/CQ timestamp 等算法通过 37 次 collection、29 次 size、6 次 path accessor 委托，不复制状态。
- [x] `[COMPAT]` Store 公开 `get_store_path`、`get_mapped_file_size_config`、`get_mapped_files` 返回语义保持不变；
  `SingleConsumeQueue` 唯一直接读取旧 size 字段的 caller 改走既有 getter。ArcSwap copy-on-write 更新、segment 排序、
  offset 计算、AllocateMappedFileService fallback 与错误/日志行为未更改。
- [x] `[TEST]` Local 新增 path/size/collection identity 与 backend interior-mutability 2/2；
  `cargo test -p rocketmq-store-local --all-features` 全量通过，`cargo test -p rocketmq-store --lib` 535/535 通过。
- [x] `[CONTRACT]` 新增 baseline+8 类 mutation 两项契约，冻结 Local 三字段/构造/accessor、dependency-free、唯一
  module/definition、Store direct import/单 owner/两个构造器/72 次 accessor，并拒绝旧字段、direct bypass、alias 与
  regression 删除；canonical 文件全集同步加入 `queue_storage.rs`。定向 3/3、完整 M06 contract 138/138 通过
  （610.352s）。
- [x] `[REV/ARCMUT]` `python scripts/arc_mut_guard.py` 直接通过，无 identity/occurrence/fingerprint 变化，ledger
  保持 1,171 identities / 3,233 occurrences，无需 ADR-013 relocation approval 或 baseline 更新。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、architecture dependency guard、AGENTS routing
  与 ArcMut guard 通过；error architecture 与既有基线一致，仍为 1 处 Broker source stringification、8 处 MCP
  `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03ao 只迁移 MappedFileQueue storage identity/collection owner，不迁移或修改 queue
  algorithms、AllocateMappedFileService、`try_flush`/`FlushProgress`、CQ timestamp 或 M06-04 flush/group-commit。
  顶层 PR-M06-03 仍需收口 CommitLog 根结构、MappedFileQueue algorithms/allocate adapter、append/recovery 方法 owner
  与 Store facade；82 个顶层工作包仍为 30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03ap MappedFileQueue index algorithm owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增 runtime-neutral `mapped_file::queue_index`，成为相邻 segment 连续性检查、offset
  range overlap、last-modified timestamp lookup 与 direct-index/linear-fallback offset lookup 四类算法的唯一 owner；
  模块不依赖 ArcSwap、DefaultMappedFile、Store/Common、runtime、tracing 或 allocate service。
- [x] `[DEV/ADAPTER]` Store `MappedFileQueue` 的 `check_self`、`range`、`get_mapped_file_by_time` 与
  `find_mapped_file_by_offset` 只负责读取 ArcSwap 快照、投影 DefaultMappedFile 字段、记录错误和克隆返回对象。
  offset lookup 使用 Local `MappedFileQueueIndex::{First, Indexed}`，保留原实现分别捕获 first/last、再读取当前集合
  的并发观察时序及 return-first fallback 对象身份。
- [x] `[COMPAT]` 相邻损坏日志可继续报告全部 pair；range 仍为 `[from,to)`；timestamp lookup 仍选择首个
  `last_modified >= timestamp`，否则返回最后文件；offset lookup 仍先 O(1) 直索引、再线性回退，并保持边界外/
  gap/return-first 语义。storage accessor 当前冻结为 collection/size/path = 37/25/6。
- [x] `[TEST]` Local 新增连续性、range、timestamp、direct/fallback/gap/first policy 4/4；Local all-features
  全量与 Store lib 535/535 通过。
- [x] `[CONTRACT]` 新增 baseline+6 类 mutation 两项契约，冻结四个 Local function、选择 enum、无依赖 owner、
  direct exact imports、四个 Store adapter 与四组回归测试，并同步 canonical file/item 集合；定向 5/5，完整 M06
  contract 140/140 通过（604.207s）。旧 M06-03ao storage contract 同步收紧为迁移后的 37/25/6 accessor。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、architecture dependency guard、AGENTS routing
  与 ArcMut guard 通过；error architecture 与既有基线一致，仍为 1 处 Broker source stringification、8 处 MCP
  `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03ap 未迁移 load/create/delete/truncate/warmup/swap、AllocateMappedFileService、
  `try_flush`/`FlushProgress`、CQ timestamp、CommitLog 或持久格式。顶层 PR-M06-03 仍需收口 CommitLog 根结构、
  MappedFileQueue I/O/allocate adapter 及剩余算法、append/recovery 方法 owner 与 Store facade；82 个顶层工作包仍为
  30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03aq MappedFileQueue allocation decision owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增无依赖 `mapped_file::queue_allocation`，用 `MappedFileQueueLastFile` 与
  `MappedFileQueueRollFile` 分别承载预分配前和预分配后快照；`plan_mapped_file_queue_preallocation` 唯一拥有 80%
  threshold/非满段 next-offset 决策，`plan_mapped_file_queue_creation` 唯一拥有空队列 start alignment、满段 roll 与
  `need_create` 门控。
- [x] `[DEV/ADAPTER]` Store `get_last_mapped_file_mut_start_offset` 只投影 DefaultMappedFile 状态并执行副作用：
  先读取 wrote/full 调 Local preallocation decision、调用 AllocateMappedFileService adapter，再重新读取 full/offset
  调 Local creation decision，最后按结果调用 `try_create_mapped_file`。
- [x] `[COMPAT]` 保留 usage ratio `wrote as f64 / segment_size as f64 >= 0.8`、full 时不预分配、空队列
  `start - start % segment_size`、满段 `file_from + segment_size`、零 segment 空队列 panic 与 `need_create=false`
  仍执行预分配的旧语义。两阶段拆分保留预分配副作用前后两次独立 `is_full()` 观察；storage accessor 当前冻结为
  collection/size/path = 37/24/6。
- [x] `[TEST]` Local 新增空队列对齐、80% boundary、满段 roll、禁创建仍预分配、零 segment 失败 5/5；Local
  all-features 全量与 Store lib 535/535 通过。
- [x] `[CONTRACT]` 新增 baseline+7 类 mutation 两项契约，冻结两个 snapshot 字段/API、两条纯决策、无依赖 owner、
  direct exact imports、Store 两阶段 adapter 顺序与五组回归测试；定向 4/4，完整 M06 contract 142/142 通过
  （619.663s）。旧 storage contract 同步收紧为 37/24/6 accessor。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、architecture dependency guard、AGENTS routing
  与 ArcMut guard 通过；error architecture 与既有基线一致，仍为 1 处 Broker source stringification、8 处 MCP
  `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03aq 未迁移 AllocateMappedFileService worker/request table、路径格式、实际 mmap/create、
  load/delete/truncate/warmup/swap、flush/CQ timestamp、CommitLog 或持久格式。顶层 PR-M06-03 仍需收口 CommitLog
  根结构、MappedFileQueue I/O/allocate adapter 及剩余算法、append/recovery 方法 owner 与 Store facade；82 个顶层工作包
  仍为 30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03ar mapped-file allocation request identity owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增无依赖 `mapped_file::allocation_request::MappedFileAllocationRequestKey`，唯一持有
  allocation request 的 full path 与 i32 file size，并成为 accessor、平台分隔符 file-offset 解析、legacy Display、
  path+size Eq 及 offset-only reverse Ord 的 canonical owner。
- [x] `[DEV/ADAPTER]` Store `AllocateRequest` 删除直接 `file_path`/`file_size` 字段和 offset 解析算法，只保留一个
  Local key、async Notify、blocking Condvar、completed flag 与 mapped-file result；constructor/getter/Display/Eq/Ord
  均窄委托 Local key，request table、BinaryHeap 和 worker lifecycle 继续由 Store 持有。
- [x] `[COMPAT]` 保留 `MAIN_SEPARATOR` 之后 i64 解析、裸文件名/无效文件名回退 0、精确
  `AllocateRequest[file_path=...,file_size=...]` 文本、path+size 完整相等与相同 offset 即排序相等的旧语义；BinaryHeap
  仍使较小 offset 先出队，未顺带修正既有 Eq/Ord 差异。
- [x] `[TEST]` Local 新增 identity/display、平台 path parsing、BinaryHeap 100/200/300 顺序与同 offset Eq/Ord 差异
  4/4；Store adapter 1/1、allocation/CommitLog 相关 4/4、Local all-features 全量及 Store lib 536/536 通过。
- [x] `[CONTRACT]` 新增 baseline+8 类 mutation 两项契约，冻结 Local fields/API/parser/Display/Eq/Ord、无依赖 owner、
  direct exact import、Store 五字段 runtime shell、全部窄委托与五组回归测试；定向 3/3，完整 M06 contract 144/144
  通过（625.228s）。canonical file/item 集合同步加入 `allocation_request.rs` 与 key owner。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、architecture dependency guard、AGENTS routing
  与 ArcMut guard 通过；error architecture 与既有基线一致，仍为 1 处 Broker source stringification、8 处 MCP
  `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03ar 未迁移 AllocateMappedFileService request table/queue、Notify/Condvar、timeout/retry、
  worker lifecycle、TransientStorePool、实际 mmap/create、MappedFileQueue load/delete/flush/CQ timestamp、CommitLog 或
  持久格式。顶层 PR-M06-03 仍需收口 CommitLog 根结构、MappedFileQueue I/O/allocate adapter 及剩余算法、append/
  recovery 方法 owner 与 Store facade；82 个顶层工作包仍为 30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03as mapped-file allocation and warm-up policy owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增 runtime-neutral `mapped_file::allocation_policy`，`MappedFileWarmupConfig` 唯一持有
  warm enable、CommitLog file-size threshold、`FlushDiskType` 与 least-pages 值；`MappedFileAllocationPoolSnapshot`
  与 `mapped_file_allocation_capacity` 唯一拥有 transient-pool/fast-fail 门控和 available-minus-queued 饱和容量决策。
- [x] `[DEV/ADAPTER]` Store 删除私有 `WarmMappedFileConfig` 和两处 capacity 算法；构造时只把
  `MessageStoreConfig` 值投影到 Local config，worker 创建文件时只读取 Local accessors。async 双文件提交与 background
  单文件提交统一调用 `allocation_capacity`，该 adapter 只按原顺序读取 queue length、pool available buffers 并构造 Local
  snapshot，不接管 Local 策略。
- [x] `[COMPAT]` disabled 仍使用 AsyncFlush/`usize::MAX`/0 且永不预热；enabled 仍以
  `file_size as usize >= mapped_file_size_commit_log` 为阈值。未启用 transient pool、未启用 fast-fail 或 pool 缺失时继续
  返回入口默认容量；受约束时仍用 `available_buffers.saturating_sub(queued_requests)`，双提交容量为 2、background 容量为 1，
  保留原 queue-before-pool 观察顺序和耗尽时的 warning/cleanup 行为。
- [x] `[TEST]` Local 新增 disabled defaults、threshold/flush values、非约束 default 与饱和容量 4/4；Store 新增 runtime
  snapshot adapter 1/1，AllocateMappedFileService/CommitLog 相关 5/5、Local all-features 全量及 Store lib 537/537 通过。
- [x] `[CONTRACT]` 新增 baseline+7 类 mutation 两项契约，冻结 Local config/snapshot fields、accessors、容量语义、唯一
  owner 与依赖、Store direct exact imports/field/constructor/两个入口 adapter 及五组回归；定向 2/2，完整 M06 contract
  146/146 通过（613.440s）。canonical file/item 集合同步加入 `allocation_policy.rs`、两个 value owner 与 capacity function。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、architecture dependency guard、AGENTS routing、
  ArcMut guard 与 enforcing runtime audit 通过；error architecture 与既有基线一致，仍为 1 处 Broker source
  stringification、8 处 MCP `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03as 未迁移 AllocateMappedFileService request table/queue、Notify/Condvar、timeout/retry、
  worker lifecycle、TransientStorePool 实体、实际 mmap/create、MappedFileQueue load/delete/flush/CQ timestamp、CommitLog 或
  持久格式。顶层 PR-M06-03 仍需收口 CommitLog 根结构、MappedFileQueue I/O/allocate adapter 及剩余算法、append/
  recovery 方法 owner 与 Store facade；82 个顶层工作包仍为 30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03at mapped-file queue dirty-tail and reset planning evidence

- [x] `[DEV/OWNER]` Local 新增无 Store/runtime 依赖的 `mapped_file::queue_maintenance`；
  `mapped_file_queue_truncate_action` 唯一拥有 completed/target/later-segment dirty-tail 分类与 target modulo position，
  `plan_mapped_file_queue_reset` 唯一拥有 two-segment 回退窗口、reverse target scan、reset position 和 removal-index 规划。
- [x] `[DEV/ADAPTER]` Store `truncate_dirty_files` 逐文件投影 offset 后按 Local enum 执行 position 更新或
  destroy/collection removal；`reset_offset` 先独立捕获 last-file offset/wrote snapshot，再加载当前 ArcSwap collection，
  将 Local plan 的 target position 和 newest-to-oldest removal indices 按原顺序应用。Local 不持有或返回
  `DefaultMappedFile`、Arc/ArcSwap、mmap、destroy 或 tracing 类型。
- [x] `[COMPAT]` truncate 仍以 `file_from + segment_size <= offset` 保留完整段、用
  `offset % segment_size` 同步 wrote/committed/flushed position，并 destroy 后续段。reset 仍只拒绝
  `last_offset - offset > segment_size * 2`，保留 last/current 两次独立集合观察、目标文件自身 size 的 modulo、
  reverse scan 与 legacy removal index 的 newest-to-oldest 生成/Store reverse 应用顺序；未顺带修复或改变既有边界行为。
- [x] `[TEST]` Local 新增 truncate 三分类、two-segment 拒绝、target/removal order 与 offset-before-queue 4/4；
  Store 新增真实 mapped-file truncate/reset adapter 2/2，验证 position、destroy availability、Arc identity 和集合结果；
  Local all-features 全量及 Store lib 539/539 通过。
- [x] `[CONTRACT]` 新增 baseline+8 类 mutation 两项契约，冻结 Local enum、last snapshot、plan fields/API、truncate/reset
  算法、无依赖 owner、四条 Store direct imports、两条副作用 adapter 与六组回归；定向 2/2，完整 M06 contract
  148/148 通过（630.047s）。canonical file/item 集合同步加入 `queue_maintenance.rs` 的三个 value owner 与两个 function；
  MappedFileQueue storage accessor 更新为 collection/size/path = 37/21/6。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、architecture dependency guard、AGENTS routing、
  ArcMut guard 与 enforcing runtime audit 通过；error architecture 与既有基线一致，仍为 1 处 Broker source
  stringification、8 处 MCP `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03at 未迁移 MappedFileQueue load/create/time/offset delete/swap/shutdown/destroy、
  flush/commit/CQ timestamp，未迁移 AllocateMappedFileService worker/request lifecycle、CommitLog 根 owner 或持久格式。
  顶层 PR-M06-03 仍需收口上述 MappedFileQueue I/O/剩余算法、CommitLog 根结构、append/recovery 方法 owner 与 Store facade；
  82 个顶层工作包仍为 30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03au AllocateMappedFileService canonical owner extraction evidence

- [x] `[DEV/OWNER]` `rocketmq-store-local::base::allocate_mapped_file_service` 成为
  `AllocateMappedFileService`、request table/BinaryHeap、async Notify、blocking Condvar、timeout、fast-fail、
  `TransientStorePool`、mapped-file create/warm-up 及 worker thread 的唯一 owner；Store 中原有同名实现与
  `services` 下的零字段占位 owner 已删除。
- [x] `[DEV/FACADE]` Store 旧路径只精确 re-export Local canonical type，并为 `MessageStoreConfig` 实现隐藏的
  Local config 投影 trait；既有 `new_with_message_store_config` 调用形态保持不变，Local 不依赖 Store config、
  Store runtime、Broker/Common/Remoting/Tiered 类型或业务编排。
- [x] `[LIFECYCLE]` Local worker 由 completion guard 在所有退出路径记录完成并唤醒 shutdown；shutdown 先等待完成通知，
  再 yield 至线程可 join 并回收 handle。实现不引入 `tokio::spawn`、`spawn_blocking`、嵌套 runtime 或 detached task；
  enforcing runtime audit 已将该专用 I/O thread 的合法边界从 Store 路径迁到 Local 路径。
- [x] `[COMPAT]` request priority、path/size identity、timeout、fast-fail capacity、双文件预分配、warm-up 阈值与 mmap/create
  语义保持不变；Store 精确类型身份 integration test 验证旧路径即 Local 类型。MappedFileQueue 预分配测试改为等待真实
  allocation result，不再依赖 service 私有 request-table 观察。
- [x] `[TEST]` Local allocation service focused 5/5、Store config facade 1/1、真实 MappedFileQueue consumer 1/1、
  Store/Local type identity 1/1 通过；Local all-feature crate unit 104/104 及全部 integration/doctest、Store lib 536/536 通过。
- [x] `[CONTRACT]` 原 allocation request/policy 契约已切换到 Local service owner；新增 service baseline 与 8 类 mutation
  契约，冻结唯一 owner、字段、worker-completion、依赖、线程/lifecycle、Store exact facade/config projection、manifest Tokio
  dependency 和回归测试。三组定向契约 6/6、完整 M06 contract 150/150 通过（698.545s）。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、diff check、architecture dependency guard、AGENTS routing、
  ArcMut guard 与 enforcing runtime audit 通过；error architecture 仍仅复现历史 1 处 Broker source stringification、8 处 MCP
  `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03au 已关闭 mapped-file allocation service owner，但未迁移 MappedFileQueue
  load/create/delete/swap/shutdown/destroy 与剩余 I/O/算法；flush/commit/CQ timestamp 属于后续 M06-04 边界。顶层
  PR-M06-03 仍需收口 CommitLog 根结构、append/recovery composition owner 与 Store facade；82 个顶层工作包仍为
  30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03av mapped-file queue load/create I/O owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增 `mapped_file::queue_io`，唯一拥有 MappedFileQueue 目录 `read_dir`、候选文件名升序排序、
  metadata/目录/segment-size 校验、尾部零长度文件删除、`DefaultMappedFile` 加载及 wrote/flushed/committed position 初始化；
  service blocking allocation、N+2 background preallocation、同步创建 fallback 与首文件标记也由 Local 执行。
- [x] `[DEV/ADAPTER]` Store `load`/`do_load` 仅提供 path、segment-size 或显式候选列表，并把 Local outcome 中已加载文件
  一次性 extend 到 ArcSwap collection；`do_create_mapped_file` 仅捕获 first-file 快照、传入两个路径并追加 Local 返回对象。
  Store production 已移除 `read_dir`、metadata、size validation、`CheetahString`、`DefaultMappedFile::try_new` 与旧
  `create_mapped_file_internal` I/O owner。
- [x] `[COMPAT]` 不存在/不可读目录仍返回成功空集合；目录项读取错误仍被过滤，目录仍跳过，最后一个零长度文件删除失败仍
  non-fatal。加载按文件名排序，三类失败均返回 failure 且保留此前成功加载的文件，Store 继续应用 partial result；allocation
  service 仅在 started 时使用，失败后仍同步 fallback，成功后仍提交 N+2，首文件仍仅在 Arc 唯一时设置标记。
- [x] `[TEST]` Local queue I/O integration 4/4、Store MappedFileQueue focused 11/11 通过；Local all-feature crate 单测
  104/104 及全部 integration/doctest 通过，Store lib 537/537 通过。新增回归覆盖 missing-dir、排序/position/空尾删除、
  partial-load failure 与 sync-create/first-file，并在 Store 验证 partial outcome 实际写入 collection。
- [x] `[CONTRACT]` 新增 queue I/O baseline 与 9 类 mutation 契约，冻结 outcome fields/API、三条 failure partial-result、
  discovery/load/create 顺序、Local 依赖边界、Store exact imports/apply adapter 和回归测试；canonical file/item 集合加入
  `queue_io.rs`、一个 value owner 与三个 public function。完整 M06 contract 152/152 通过（739.115s），storage accessor
  更新为 collection/size/path = 37/16/6。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、diff check、architecture dependency guard、AGENTS routing、
  ArcMut guard 与 enforcing runtime audit 通过；error architecture 仍仅复现历史 1 处 Broker source stringification、8 处 MCP
  `anyhow` 和 2 份缺失治理文档，本切片无新增。
- [x] `[INVENTORY/SCOPE]` M06-03av 已关闭 MappedFileQueue load/create I/O owner，但 time/offset delete、retry-delete、
  swap/clean、shutdown/destroy 与剩余 lifecycle/统计算法仍需迁移；flush/commit/CQ timestamp 保留给 M06-04。顶层 PR-M06-03
  仍需收口 CommitLog 根结构、append/recovery composition owner 与 Store facade；82 个顶层工作包仍为 30 已完成、
  1 进行中、51 未开始，即 52 个尚未完成。

## M06-03aw mapped-file queue lifecycle owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增 `mapped_file::queue_lifecycle`，唯一拥有 recovery last-file destroy、collection removal
  filtering、按时间/物理 offset 过期删除、first-file retry-delete、swap/clean reserve-window、全文件 shutdown/destroy 与目录删除。
  `MappedFileQueueDeletion` 统一返回成功 destroy 数量和需由 collection owner 移除的对象。
- [x] `[DEV/ADAPTER]` Store 删除路径只捕获 ArcSwap snapshot，time-delete 额外保留 `check_self` 与 `current_millis` 注入，
  然后按 Local deletion result 更新 collection；swap/clean 只注入时间源，shutdown/destroy 只传 snapshot/path。Store production
  不再读取 CQ tail bytes、执行 destroy/sleep/swap/shutdown 或删除目录；destroy adapter 在 Local 完成副作用后清空 collection 并归零
  flushed watermark。
- [x] `[COMPAT]` time-delete 继续永不删除最新文件、oldest-first、按 batch/interval 限制并在首个 live/destroy-failed 文件停止；
  offset-delete 继续读取最后一个 CQ unit 的 big-endian physical offset、在首个 retained/失败对象停止。当前 select 后 destroy 失败即
  返回 0 的既有行为由回归冻结，未在迁移中修复。retry-delete 仍只处理 unavailable first file；swap 仍至少保留 3 个最新文件并按
  newest-to-oldest 反向候选顺序执行 force/normal interval。
- [x] `[TEST]` Local lifecycle integration 6/6、Store MappedFileQueue focused 11/11 通过；Local all-feature crate 单测
  104/104 及全部 integration/doctest、Store lib 537/537 通过。覆盖 current-snapshot removal、last destroy、time batch/newest
  retention、offset destroy-failure stop、swap reserve/shutdown 与 directory destroy。
- [x] `[CONTRACT]` 新增 lifecycle baseline 与 10 类 mutation 契约，冻结 deletion result、九个 Local function、时间/offset
  删除顺序、retry gate、swap reserve、Store exact imports/adapters 与回归测试；canonical file/item 集合加入
  `queue_lifecycle.rs`、一个 value owner 与九个 function。完整 M06 contract 154/154 通过（744.490s），storage accessor
  更新为 collection/size/path = 36/16/6。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、diff check、architecture dependency guard、AGENTS routing、
  ArcMut guard 与 enforcing runtime audit 通过；本切片未修改 typed error mapping 或敏感字段，最近 error architecture 结果仍仅为
  历史 1 处 Broker source stringification、8 处 MCP `anyhow` 和 2 份缺失治理文档。
- [x] `[INVENTORY/SCOPE]` M06-03aw 已关闭 MappedFileQueue deletion/swap/shutdown/destroy lifecycle owner；剩余
  max/min/size/fall-behind/warmup/lazy-mmap 等统计/纯计算，以及 flush/commit/CQ timestamp 边界。后者保留给 M06-04/05。
  顶层 PR-M06-03 仍需收口 CommitLog 根结构、append/recovery composition owner 与 Store facade；82 个顶层工作包仍为
  30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03ax mapped-file queue metrics and query owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增 `mapped_file::queue_metrics`，成为 `MappedFileWarmupStats`、warmup/lazy-mmap 聚合、
  readable/wrote max offset、roll decision、empty min-offset sentinel、available mapped-memory、fall-behind 与 total configured bytes
  的唯一 owner。Store 删除 copied stats 类型，并从 Local 精确 public re-export 保持旧 API 路径。
- [x] `[DEV/ADAPTER]` Store `warmup_stats`/`lazy_mmap_stats`、max/min/roll/memory/fall-behind/total 与内部 max-wrote 方法
  只捕获 first/last/collection/watermark snapshot 并调用一条 Local function；不再读取 metrics fields、mapped-file position/file-size、
  availability 或复制聚合/算术算法。
- [x] `[COMPAT]` 空队列 max/max-wrote/fall-behind 仍为 0、min 仍为 -1、roll 仍为 true；写入恰好填满剩余空间时不提前 roll，
  只有 `>` 才 roll。available memory 继续排除 destroyed files，total size 继续按全部 collection count；flushed watermark 为 0 时
  fall-behind 仍为 0。warmup totals 继续饱和累加并保留最后一个有操作文件的 latency，lazy-mmap stats 继续逐文件饱和聚合。
- [x] `[TEST]` Local metrics integration 5/5、Store MappedFileQueue focused 11/11 通过；Local all-feature crate 单测
  104/104 及全部 integration/doctest、Store lib 537/537 通过。覆盖 empty sentinels、exact roll boundary、read/wrote offsets、
  available/total 区分、warmup aggregate 与 lazy mapped/unmapped aggregate。
- [x] `[CONTRACT]` 新增 metrics baseline 与 10 类 mutation 契约，冻结 stats fields/traits、九个 Local function、边界/聚合语义、
  Store exact imports/re-export/adapters 与回归测试；canonical file/item 集合加入 `queue_metrics.rs`、一个 value owner 与九个 function。
  完整 M06 contract 156/156 通过（748.103s），storage accessor 更新为 collection/size/path = 34/16/6。
- [x] `[REV]` Local/Store all-target/all-feature Clippy、workspace fmt、diff check、architecture dependency guard、AGENTS routing、
  ArcMut guard 与 enforcing runtime audit 通过；本切片未修改 typed error mapping 或敏感字段，最近 error architecture 结果仍仅为
  历史 1 处 Broker source stringification、8 处 MCP `anyhow` 和 2 份缺失治理文档。
- [x] `[INVENTORY/SCOPE]` MappedFileQueue 在 PR-M06-03 范围内的 storage/index/allocation/load-create/lifecycle/metrics owner
  已完成；flush/commit/group-commit 与 CQ timestamp 分别保留给 M06-04/M06-05。顶层 PR-M06-03 当前剩余 CommitLog 根结构、
  append/recovery composition owner 与 Store facade 收口；82 个顶层工作包仍为 30 已完成、1 进行中、51 未开始，即
  52 个尚未完成。

## M06-03ay CommitLog root owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增泛型 `commit_log::root::CommitLogRoot<A>`，唯一拥有完整 CommitLog composition adapter，
  并只公开 immutable/mutable/consuming 三种窄借用方式；Local 根不感知 Store config、MappedFile、Broker、HA、CQ/Index 或 facade 类型。
- [x] `[DEV/FACADE]` Store 旧 public `CommitLog` 类型和路径保持不变，但结构收敛为唯一字段
  `CommitLogRoot<CommitLogAdapter>`；原 15 个 Store-specific dependency/state 字段机械集中到 `CommitLogAdapter`，
  `Deref`/`DerefMut` 只承担遗留方法的过渡适配。构造顺序、默认值、MappedFileQueue/flush-manager 共享身份均未改变。
- [x] `[COMPAT]` 两条 put-message 路径继续按 topic-queue lock → put-message lock → append-attempt 顺序执行；为保持根 facade
  下的安全拆分借用，仅克隆既有 `Arc` lock handle 后获取同一锁，不增加 task/thread/channel，也不改变 offset assignment、timestamp、
  active memory lock、EOF roll/retry、CRC 或 result mapping。`append_data` 同样仍获取原 put-message mutex。
- [x] `[TEST]` Local root integration 2/2、Store root compile 与 put-message lock focused 1/1 通过；Local all-feature crate
  104/104 及全部 integration/doctest、Store lib 537/537 通过。覆盖 adapter identity、唯一可变 owner、既有 lock statistics，
  并由 Store 全量回归覆盖 append/load/recovery/lifecycle 路径。
- [x] `[CONTRACT]` 新增 root owner/facade baseline 与 5 类 mutation，冻结 Local 单字段 generic owner、四个借用/消费方法、module export、
  Store 单字段 facade、15 字段 composition adapter、构造与 Deref flow、两个回归测试；原 runtime-state contract 改为验证 Local root 所拥有
  adapter 内仍只有一个 canonical `CommitLogRuntimeState`。定向 5/5 与完整 M06 contract 158/158 通过（759.554s）。
- [x] `[GOVERNANCE/REV]` `ArcMut` owner relocation 经 6 条逐项 approval 和生成式 baseline promotion 审核，迁移前后均为
  1,171 个 identity、3,233 个 occurrence，债务零增长；ArcMut guard 复跑通过。Local/Store all-target/all-feature Clippy、
  workspace fmt、diff check、architecture dependency guard、AGENTS routing 与 enforcing runtime audit 通过。本切片未修改 typed error
  mapping 或敏感字段，最近 error architecture 结果仍仅为历史 1 处 Broker source stringification、8 处 MCP `anyhow` 和 2 份缺失治理文档。
- [x] `[INVENTORY/SCOPE]` 本切片关闭 CommitLog 根结构 owner；MappedFileQueue 本阶段范围也已完成。PR-M06-03 仅剩 append/recovery
  composition method owner 与 Store facade 最终收口，flush/group-commit、CQ/Index、HA、Timer/POP 仍分别保留给 M06-04..07。
  顶层统计不变：30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03az CommitLog append outcome resolution owner extraction evidence

- [x] `[DEV/OWNER]` Local `append_attempt` 新增四值 `CommitLogAppendStatus`、六值 `CommitLogAppendFailure` 与
  `CommitLogAppendResolution<S,E>`，并由 `CommitLogAppendOutcome::resolve` 穷尽拥有 2 个 completed 与 6 个 aborted outcome
  到 continue/return、append result、unlock/abandoned segment 及 error detail 的唯一映射。
- [x] `[DEV/ADAPTER]` Store batch/single 两条路径删除 8-arm outcome/status/resource mapping，统一在 Local attempt 后调用一次
  `resolve()`；Store 只把四个中立 status 投影到 legacy `PutMessageStatus`，按 failure detail 保留原日志，按 Local 决策释放 request lock、
  topic lock 和 abandoned segment，并继续既有 offset/stats/flush/HA 后处理。
- [x] `[COMPAT]` PutOk、retry rejected、initial unavailable/lock/message-illegal/unknown、rolled unavailable/lock 的 legacy status、
  append-result presence、EOF old-segment lifetime和日志顺序均不变；Local resolution 不 clone segment/result/error，不增加第三次 append，
  Store 仍在 failure 日志之后 drop abandoned segment，并在任何 return 前释放两把 request lock。
- [x] `[TEST]` Local append-attempt integration 10/10、Local all-feature crate 单元 104/104 及全部 integration/doctest、Store
  single/batch EOF encoded-buffer ownership focused 2/2 与 Store lib 537/537 通过；新增 completed/aborted 穷尽 resolution 回归。
- [x] `[CONTRACT]` append-attempt contract 扩展为冻结三个 resolution 类型、8-arm mapping、status/result/segment/error 所有权、
  Store exact imports、一次 resolve、四值 legacy status adapter、failure logging/cleanup 顺序和两个新增回归；定向 baseline + mutation
  2/2 与完整 M06 contract 158/158 通过（758.101s）。Local/Store all-target/all-feature strict Clippy、workspace fmt、
  architecture dependency guard、ArcMut guard、AGENTS routing、enforcing runtime audit 与 diff check 均通过。
- [x] `[INVENTORY/SCOPE]` append attempt/roll/retry/terminal resolution composition 已由 Local 拥有；Store 留存 message admission/encode、
  lock acquisition、legacy result facade 与 M06-04/M06-06 的 flush/HA port。PR-M06-03 继续收口 recovery completion owner 与最终 facade；
  顶层统计保持 30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03ba CommitLog recovery completion owner extraction evidence

- [x] `[DEV/OWNER]` Local 新增 `recovery::CommitLogRecoveryCompletion`，唯一拥有 empty/recovered completion 词汇、normal/abnormal
  summary 到 signed confirm/controller/process offset 的映射，以及 ConsumeQueue truncation 决策。normal standard/optimized 的 controller
  上界差异由各自 state policy 保留；abnormal controller 上界继续来自 confirm-valid watermark。
- [x] `[DEV/ADAPTER]` Store 四条 recovery 路径删除重复的 summary conversion、controller/legacy confirm 选择、CQ truncation 判断与
  queue progress 写回，统一各调用一次 Local `completion(...)`；单一 `apply_recovery_completion!` adapter 只应用 controller clamp、legacy confirm、
  CQ truncate、flushed/committed 与 dirty-file truncate 外部副作用，并处理 empty reset/destroy/reload。
- [x] `[COMPAT]` normal standard 仍以 process offset 作为 controller recovery 上界、normal optimized 仍以 last-valid offset 为上界；
  abnormal 两条路径仍以 confirm-valid offset 为 controller 上界、last-valid offset 为非 controller confirm。负 CQ max offset、`>=`
  截断边界、warning 与 truncate 顺序、空 CommitLog 的路径专属日志和 reset/destroy/reload 顺序均保持不变。
- [x] `[TEST]` Local completion integration 2/2、Local all-feature crate 单元 104/104 及全部 integration/doctest、Store lib 537/537
  通过；覆盖 normal policy-specific controller boundary、abnormal confirm/truncate watermark、负值与精确 CQ 边界。
- [x] `[CONTRACT]` 新增 completion owner baseline + 6 类 mutation，冻结 enum shape、signed-offset state invariant、normal/abnormal mapping、
  module export、两项回归以及 Store 单一 side-effect adapter；同步将旧 direct-summary/CQ 契约改为禁止 Store 回流决策。最终形态关键定向
  baseline/mutation 5/5 与完整 M06 contract 159/159 通过（745.397s）。Local/Store all-target/all-feature strict Clippy、workspace fmt、
  architecture dependency guard、ArcMut guard、AGENTS routing、enforcing runtime audit 与 diff check 均通过；本切片未修改 typed-error
  mapping 或敏感字段。
- [x] `[INVENTORY/SCOPE]` CommitLog recovery completion composition 已由 Local 拥有；Store 只保留 MappedFile/CQ/runtime state 外部副作用
  adapter。PR-M06-03 仅剩最终 Store facade/legacy ledger 收口；flush/group-commit、CQ/Index、HA、Timer/POP 仍留给 M06-04..07。
  顶层统计保持 30 已完成、1 进行中、51 未开始，即 52 个尚未完成。

## M06-03bb PR-M06-03 facade and ledger closeout evidence

- [x] `[ENTRY/SCOPE]` M02 `try_flush`、dirty-tail/recovery golden 与 crash-before-flush 基线保持冻结；本父 PR 未迁移或修改
  flush/group-commit、CQ/Index、HA、Timer/POP 或 `LocalFileMessageStore` composition，这些范围继续由 M06-04～08 独立承接。
- [x] `[DEV/OWNER]` `rocketmq-store-local` 保持 `default = []` 并 canonical 拥有 fast-load/safe-load/io_uring、MappedFile/Queue、
  AllocateMappedFileService、CommitLog append/load/recovery/runtime/root；Store `CommitLog` 为单字段 Local root facade，旧深路径继续通过
  精确 re-export、type alias、config 投影和外部副作用 adapter 保持。
- [x] `[LEDGER/COMPAT]` 新增 [`06-storage-local-compatibility-ledger.md`](06-storage-local-compatibility-ledger.md)，冻结七组 owner/facade、
  feature forwarding、M06-04～08 保留 port、下一 major 删除条件和整体回滚规则；新增 source contract 将 ledger 与两个 manifest、
  单字段 CommitLog facade、MappedFile/recovery/allocation-service re-export 绑定，并拒绝 5 类漂移。
- [x] `[ISSUE]` GitHub [#8200](https://github.com/mxsm/rocketmq-rust/issues/8200) 记录父项目标、已完成范围、兼容边界、验收证据与回滚规则；
  后续 PR 将以 `Closes #8200` 关联。
- [x] `[TEST/REV]` ledger 定向 baseline/mutation 1/1 与完整 M06 owner/adapter/mutation contract 160/160 通过（741.912s）。
  最终 Rust 快照的 Local all-feature 单元 104/104 及全部 integration/doctest、Store lib 537/537、Local/Store all-target/all-feature
  strict Clippy、workspace fmt、architecture dependency、ArcMut zero-growth、AGENTS routing、enforcing runtime audit 与 diff check 均通过。
- [x] `[MAIN/ROLLBACK]` `git fetch origin main` 后分支相对 `origin/main` 为 0 behind；旧 public path、feature 入口和磁盘格式未改变，
  因此可整体 revert 父 PR。回滚不得恢复已由 contract 禁止的 Local/Store 双 owner。
- [x] `[INVENTORY]` PR-M06-03 父项关闭，M06 整体仍进行中且 Exit Checklist 保持未完成。82 个顶层工作包更新为
  31 已完成、0 进行中、51 未开始，即 51 个尚未完成；下一工作包为 PR-M06-04。

## M06-04a GroupCommit request and runtime stats owner extraction evidence

- [x] `[ENTRY]` `failed_canonical_flush_marks_store_unwriteable_and_legacy_flush_keeps_watermark` 1/1、
  `default_flush_manager::tests` 9/9 与 `store_api_legacy_adapter` 9/9 在迁移前通过；canonical `try_flush`、R0
  `flush() -> i64` adapter、SyncFlush/ack 和 typed failure 基线均未发现待回 M02 的缺陷。
- [x] `[DEV/OWNER]` 新增 `rocketmq-store-local::flush::group_commit`，唯一拥有泛型 `GroupCommitRequest<E>`、
  neutral `GroupCommitStatus`、batch success/error completion、deadline/enqueue timestamp 与 `SyncFlushStats`；Store 内部
  `group_commit_request` 收敛为 `GroupCommitRequest<StoreError>` type alias，公开 `SyncFlushRuntimeInfo` 旧路径改为精确 re-export。
- [x] `[ADAPTER/COMPAT]` Store `DefaultFlushManager` 继续把 `Flushed/TimedOut` 穷尽映射为
  `PutOk/FlushDiskTimeout`，保留同批单次 flush、typed error `Arc` 身份、health recorder 先记录一次、queue-depth/wait-time
  统计和 timeout 行为；未修改 channel 容量 1024、最多 1000 次重试、1ms 间隔、fsync 策略、checkpoint 或默认配置。
- [x] `[TEST/CONTRACT]` Local 新增 final-watermark 双 waiter、共享 typed error 和 zero-timeout 3/3；Store 原 GroupCommit
  回归 9/9。新增 `test_m06_flush_local_contract.py` 1/1，锁定六个 canonical owner、Store type alias/re-export/import 和
  禁止 duplicate struct/batch/stats implementation。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency、AGENTS routing、
  enforcing runtime audit 与 diff check 全部通过。ArcMut 仅因新增 Local import 产生既有 `ArcMut`/`WeakArcMut` 各一处 token
  relocation，按 ADR-013 一对一 approval 更新后仍为 1,171 identities/3,233 occurrences，零新增债务。
- [x] `[INVENTORY/SCOPE]` M06-04a 只关闭 request/batch/runtime-stats owner；`FlushProgress`、queue flush/commit I/O、
  GroupCommit worker/checkpoint、AsyncFlush/CommitRealTime worker 和最终 facade 分别留给 M06-04b～e。82 个顶层工作包为
  31 已完成、1 进行中、50 未开始，即仍有 51 个尚未完成。

## M06-04b mapped-file queue flush/commit driver extraction evidence

- [x] `[DEV/OWNER]` 新增 `rocketmq-store-local::flush::queue`，唯一拥有 `FlushProgress`、segment flush/commit outcome、
  final durable watermark/timestamp 计算和 legacy commit-result 判定；Store `consume_queue::mapped_file_queue::FlushProgress`
  改为 canonical Local 类型的精确 re-export。
- [x] `[ADAPTER/COMPAT]` Store `MappedFileQueue` 继续提供 ArcSwap snapshot、具体 mapped-file 查找、`try_flush`/`commit`
  I/O 和 Local runtime-state 写回；Local driver 注入 offset/return-first 参数并保持原有 `file_from_offset + position` 算法。
  `flush_least_pages == 0` 才替换 timestamp，I/O error 在写回 durable watermark 前原样返回，legacy `flush()` 和 `commit()`
  布尔语义均未改变。
- [x] `[TEST]` Local 新增 empty/thorough/partial/error flush 与 commit legacy contract 5/5；Store MappedFileQueue 原回归
  11/11、FlushManager 原回归 9/9，并新增 legacy/canonical `FlushProgress` 编译期类型身份 1/1。
- [x] `[CONTRACT/REV]` M06 flush owner contract 扩展为 12 个 queue/group-commit owner 与 Store re-export/driver adapter，
  定向 1/1。Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency、ArcMut zero-growth、
  AGENTS routing、enforcing runtime audit 和 diff check 全部通过；本切片没有 ArcMut baseline 变化。
- [x] `[SCOPE]` 未修改 batch/channel/retry/fsync/checkpoint/default config；GroupCommit worker/checkpoint、
  AsyncFlush/CommitRealTime worker 和最终 facade 继续由 M06-04c～e 承接。顶层统计仍为 31 已完成、1 进行中、
  50 未开始，即 51 个尚未完成。

## M06-04c GroupCommit worker and checkpoint owner extraction evidence

- [x] `[DEV/OWNER]` Local `run_group_commit_worker` 现唯一拥有 request channel drain、同批最大 target offset、取消时尾批
  flush、forced/I/O error fan-out、最多 1000 次 flush、1ms retry policy、deadline stop 和正 timestamp checkpoint 判定；
  `GROUP_COMMIT_CHANNEL_CAPACITY=1024` 与 `GroupCommitWorkerConfig::legacy()` 冻结原 R0 参数。
- [x] `[ADAPTER/RUNTIME]` Store `GroupCommitService::start` 只创建既有 TaskGroup/channel，并注入 flush I/O、当前 durable
  watermark/timestamp、`StoreCheckpoint::set_physic_msg_timestamp`、health recorder 与 timer adapter。Local 不创建 runtime/task，
  retry sleep 通过 Store scheduler adapter 执行，enforcing runtime audit 因而保持零 action-required 增量。
- [x] `[COMPAT]` blocking-pool flush helper 直接返回 canonical Local `FlushProgress`，不再复制中间 result；Store error mapping
  仍保留 `MappedFileError -> StoreError` 和 runtime task failure -> `InvalidState`，同一 `Arc<StoreError>` 继续同时提供给 health
  recorder 与全批 waiter。channel、batch、重试、checkpoint、fsync 和 shutdown 顺序均未改变。
- [x] `[TEST/CONTRACT]` Local GroupCommit suite 从 3 扩展为 5/5，新增双 waiter worker batching/checkpoint 与 forced-error
  单次记录/共享 `Arc`；Store FlushManager 原回归 9/9。source contract 新增 Local worker/config/ports owner，并禁止 Store
  留存 `for 0..1000` 和 `rx_in.try_recv` worker 算法。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency、AGENTS routing、
  enforcing runtime audit 和 diff check 通过。ArcMut 3 个既有 occurrence（测试 glob、flush helper `ArcMut` 参数、
  `WeakArcMut` import）按 ADR-013 一对一 relocation 后仍为 1,171 identities/3,233 occurrences，零新增债务。
- [x] `[SCOPE]` 本切片不迁移 AsyncFlush/CommitRealTime worker 或最终 FlushManager facade；它们继续由 M06-04d/e
  承接。顶层统计保持 31 已完成、1 进行中、50 未开始，即 51 个尚未完成。

## M06-04d AsyncFlush and CommitRealTime worker owner extraction evidence

- [x] `[DEV/OWNER]` 新增 Local `flush::worker`，由 `run_flush_real_time_worker` 唯一拥有 periodic/thorough interval、
  final flush、checkpoint 判定与 failure phase 分类；`run_commit_real_time_worker` 唯一拥有 commit loop、thorough interval、
  final commit、flush wakeup 和 checkpoint 判定。两类 legacy config 均由 Store 配置投影一次后传入 Local。
- [x] `[ADAPTER/RUNTIME]` Store 继续拥有 `TaskGroup`、`CancellationToken`、`Notify`、blocking executor、具体 mapped-file
  flush/commit I/O、`StoreCheckpoint`、health/log callback 与 `WeakArcMut<dyn FlushManager>` wakeup adapter。Local 不创建
  runtime/task 且不直接调用 timer；delay 由 Store 注入，enforcing runtime audit 保持零 action-required 增量。
- [x] `[COMPAT]` AsyncFlush 仍按原 `flush_interval`、`flush_thorough_interval` 与 timed/notify 策略运行，取消后执行原次数
  final flush；CommitRealTime 仍保持 commit 后唤醒 flush、positive timestamp checkpoint 与 final commit 顺序。旧
  `DefaultFlushManager` public facade、GroupCommit channel/retry/fsync 及错误到 health 的映射均未改变。
- [x] `[TEST/CONTRACT]` Local 新增 cancelled final flush/checkpoint、periodic failure phase + final flush、commit progress
  wakeup + final commit checkpoint 3/3；Store `default_flush_manager::tests` 原回归 9/9。M06 flush source contract 1/1
  扩展锁定两个 Local runner/config/ports owner，并禁止 Store 留存旧 timestamp 驱动循环。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency、AGENTS routing、
  enforcing runtime audit 与 diff check 通过。3 个既有 ArcMut occurrence（测试 glob、module `ArcMut` import、commit helper
  参数）按 ADR-013 一对一 relocation 后仍为 1,171 identities/3,233 occurrences，零新增债务。
- [x] `[SCOPE]` 本切片不收口最终 FlushManager facade、SyncFlush/ack adapter 或 compatibility ledger；它们继续由
  M06-04e 承接。顶层统计保持 31 已完成、1 进行中、50 未开始，即 51 个尚未完成。

## M06-04e FlushManager facade and parent closeout evidence

- [x] `[DEV/OWNER]` 新增 Local 泛型 `FlushManagerRoot<A>`，唯一拥有完整 composition adapter；Store 旧
  `DefaultFlushManager` 收敛为仅含 `FlushManagerRoot<DefaultFlushManagerAdapter>` 的单字段 facade，并通过窄
  `Deref/DerefMut` 委托保持构造、trait 与内部调用兼容。
- [x] `[ADAPTER/COMPAT]` Store adapter 继续持有 `MessageStoreConfig`、三个 service lifecycle、MappedFileQueue、
  SyncFlush stats 与 health recorder；`GroupCommitStatus` 仍穷尽映射为 `PutOk/FlushDiskTimeout`。source contract 禁止
  facade 内部调用 legacy `.flush()`，所有确认路径继续只经 `try_flush`/durable watermark。
- [x] `[LEDGER]` `06-storage-local-compatibility-ledger.md` 升级为 M06-03/M06-04 快照，冻结
  `flush::{root,group_commit,queue,worker}` canonical owner、Store-only 外部 adapter、下一 major 删除窗口与分 PR 回滚规则。
- [x] `[TEST]` Local `FlushManagerRoot` identity/mutable-owner 2/2、Store FlushManager 9/9、Local all-feature unit/integration/
  doctest 全绿、Store all-feature lib 545/545；I/O failure、同批 waiter、batching、watermark 单调性与 crash-before-flush
  均由上述 suite 覆盖。
- [x] `[CONTRACT]` M06 flush owner contract 1/1，完整 M06 owner/adapter/mutation contract 160/160（755.875s）；
  compatibility ledger 定向 mutation test 1/1。契约锁定单字段 facade、Local root、无 duplicate owner 与 feature 转发。
- [x] `[REV]` Local/Store strict Clippy、root workspace `--no-deps --all-targets --all-features -D warnings` Clippy、workspace
  fmt、architecture dependency、ArcMut zero-growth、AGENTS routing、enforcing runtime audit 和 diff check 全部退出码 0。
  唯一 facade field relocation 按 ADR-013 一对一更新后仍为 1,171 identities/3,233 occurrences，零新增债务。
- [x] `[MAIN/ROLLBACK]` `git fetch origin main` 后分支相对 `origin/main` 为 0 behind；旧 Store public path、feature、默认配置、
  batch/channel/retry/fsync/checkpoint 与磁盘格式未改变，可整体 revert PR-M06-04，但不得恢复双 owner 或 `i64` ack 判定。
- [x] `[INVENTORY]` PR-M06-04 父项关闭，M06 整体仍进行中且 Exit Checklist 保持未完成。82 个顶层工作包更新为
  32 已完成、0 进行中、50 未开始，即 50 个尚未完成；下一工作包为 PR-M06-05。

## M06-05a canonical 20-byte ConsumeQueue record extraction evidence

- [x] `[ENTRY]` Store `single_consume_queue::tests` 4/4、Index module 22/22、normal/abnormal dirty-tail recovery 2/2
  在迁移前通过；20B CQ、40B IndexHeader/20B index entry、min/max/query 与 replay 基线未发现需先行修复的缺陷。
- [x] `[DEV/OWNER]` 新增 `rocketmq-store-local::consume_queue::record`，唯一拥有 `CQ_STORE_UNIT_SIZE=20`、
  `MSG_TAG_OFFSET_INDEX=12`、`ConsumeQueueRecord`、big-endian encode/decode、checked decode-at、pre-blank sentinel 与
  written/end-offset 语义；Local root 暴露 canonical `consume_queue` module。
- [x] `[ADAPTER/COMPAT]` Store `single_consume_queue` 精确 re-export 两个旧常量路径，并在 append、pre-blank、truncate、
  recovery、min-offset correction、timestamp search 与 iterator 全部调用 Local codec；Store 不再保留 `BytesMut` 编码缓冲或
  `put/get_i64/i32`、`from_be_bytes` 的第二份 20B codec。文件路径、dispatch 顺序与 CQExt 地址判定未改变。
- [x] `[TEST/CONTRACT]` Local Java-compatible exact bytes、signed round-trip、bounded decode-at、pre-blank 4/4；Store CQ
  原回归 4/4，normal/abnormal dirty-tail 2/2。新增 M06 CQ owner contract 1/1，锁定 Local owner、Store re-export/adapter
  与禁止 duplicate codec。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency、ArcMut zero-growth、
  AGENTS routing 与 diff check 通过。既有 `ArcMut` import/field 两处按 ADR-013 一对一 relocation 后仍为
  1,171 identities/3,233 occurrences，零新增债务；本切片未触发 runtime ownership gate。
- [x] `[SCOPE]` 本切片只关闭 canonical 20B CQ record codec；Single/Batch/CQExt driver、ConsumeQueue root/store、
  IndexHeader/IndexFile/IndexService 与最终 ledger 继续由 M06-05b～g 承接。顶层统计为 32 已完成、1 进行中、
  49 未开始，即仍有 50 个尚未完成。

## M06-05b SingleConsumeQueue kernel extraction evidence

- [x] `[DEV/OWNER]` 新增 `rocketmq-store-local::consume_queue::single`，唯一拥有 fixed-record recovery scan、
  min-offset selection、truncate planning 与 timestamp lower/upper boundary search。Local kernel 只接收 record reader、
  CommitLog timestamp lookup 与 CQExt address predicate，不持有 mapped file、Store、runtime、日志或配置类型。
- [x] `[ADAPTER/COMPAT]` Store `ConsumeQueue` 保留 mapped-file 选择/位置应用、CommitLog lookup、CQExt truncate、日志与
  文件删除 adapter；原有 duplicate timestamp、首记录截断特例、不可读 record 重试、dirty-tail recovery、文件路径、
  20B 格式及 public trait/path 均未改变。Store 已无第二份 binary-search、recovery、min-offset 或 truncate record loop。
- [x] `[TEST/CONTRACT]` Local SingleConsumeQueue suite 10/10，覆盖 duplicate/missing/out-of-range timestamp、lookup failure、
  recovery prefix、min-offset、delete/retain/retry truncate；Store adapter 回归 6/6，duplicate timestamp 1/1，normal/abnormal
  dirty-tail recovery 2/2，M06 CQ source owner/adapter contract 1/1。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency baseline、ArcMut
  zero-growth、AGENTS routing 与 diff check 全部退出码 0。ArcMut ledger 保持 1,171 identities/3,233 occurrences，
  无 relocation approval 或 baseline 变化；本切片未触发 runtime、typed-error、feature 或 observability 专项门禁。
- [x] `[SCOPE]` 本切片只关闭 SingleConsumeQueue scan/search/recovery kernel；BatchConsumeQueue/CQExt、ConsumeQueue
  root/store/dispatch、IndexHeader/IndexFile/IndexService 与最终 ledger 继续由 M06-05c～g 承接。顶层统计保持
  32 已完成、1 进行中、49 未开始，即仍有 50 个尚未完成。

## M06-05c BatchConsumeQueue and CQExt kernel extraction evidence

- [x] `[DEV/OWNER]` Local `consume_queue::batch` 唯一拥有 canonical 46B `BatchConsumeQueueRecord` 与 first/last、
  queue-offset、timestamp、recovery、truncate、min-offset kernel；Local `consume_queue::extension` 唯一拥有公开
  `CqExtUnit`、可变长 big-endian codec、地址 decorate/undecorate、容量/换页、dirty-tail scan 与文件保留判定。
- [x] `[ADAPTER/COMPAT]` Store BatchCQ 只注入 mapped-file read/position、原子 offset 与日志 adapter；CQExt 只保留
  mapped-file create/append/truncate/delete、生命周期与日志。旧 `rocketmq_store::consume_queue::cq_ext_unit::{CqExtUnit,
  MIN_EXT_UNIT_SIZE,MAX_EXT_UNIT_SIZE}` 路径精确 re-export Local owner，Broker 默认特性编译通过；46B/CQExt 磁盘格式、
  dispatch 顺序、地址范围、四字节 end blank、三次 append retry 与 public trait 语义未改变。
- [x] `[TEST/CONTRACT]` Local Batch/CQExt kernel 12/12；Store BatchCQ 3/3、CQExt 4/4、SingleCQ+CqExt 6/6；M06 CQ
  owner/adapter source contract 1/1。额外覆盖 46B golden/invalid record、offset/time/min/recovery/delete/retain/retry、
  CQExt round-trip/truncation/partial tail、public mutator/write-to/skip、address/capacity/roll boundary。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、architecture dependency baseline 与 ArcMut final guard
  通过；Broker default-feature `cargo check` 通过。CQExt module 既有 `ArcMut` import 因 Local imports 改变相邻指纹，
  按 ADR-013 完成 1 条一对一 relocation approval，ledger 保持 1,171 identities/3,233 occurrences，零新增债务。
  首次 Broker all-feature check 仅因 124 秒工具超时未形成结果，未标记为通过；随后默认特性检查退出码 0。
- [x] `[SCOPE]` 本切片不迁移 ConsumeQueue root/store/dispatch、IndexHeader/IndexFile/IndexService 或最终 ledger；
  它们继续由 M06-05d～g 承接。未修改 manifest、feature、runtime ownership、typed-error、observability、持久路径或
  默认配置。顶层统计保持 32 已完成、1 进行中、49 未开始，即仍有 50 个尚未完成。

## M06-05d ConsumeQueue root, store, and dispatch extraction evidence

- [x] `[DEV/OWNER]` Local `consume_queue::root` 新增 canonical `ConsumeQueueRoot<A>`/`ConsumeQueueStoreRoot<A>` 与
  `ConsumeQueueDispatchRoot<A>`，唯一拥有 transaction eligibility gate、global progress projection、Single/Batch
  validation/writeability/30 次 retry driver、lock-free find→out-of-lock create→atomic publish 顺序与 query clamp。
- [x] `[ADAPTER/COMPAT]` Store `ConsumeQueueStore` 收敛为 Local root 包装既有 `ArcMut<Inner>` 的 composition facade；
  `CommitLogDispatcherBuildConsumeQueue` 收敛为 Local dispatch root facade。Store 继续拥有 MessageSysFlag/CQType 投影、
  `DispatchRequest`、mapped-file/CQ factory、DashMap/ArcMut table、配置、checkpoint、CQExt、multi-dispatch、日志与 error flag；
  public trait/path、transaction gate、30 次重试、checkpoint/LMQ 顺序和并发 triple-check 行为未改变。
- [x] `[TEST/CONTRACT]` Local ConsumeQueue root 7/7；Store SingleCQ 6/6、BatchCQ 4/4、ConsumeQueueStore 9/9、
  transaction dispatcher 1/1；M06 CQ owner/adapter source contract 1/1。覆盖 retry success/exhaustion、invalid Batch、
  non-writeable、find hit/miss publish、offset clamp、prepared/rollback skip 与 empty progress。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、architecture dependency baseline、ArcMut final guard、
  AGENTS routing、workspace fmt 与 diff check 通过。Local root 包装和 factory driver 仅使 4 个既有同 item occurrence
  改变上下文，按 ADR-013 一对一批准后 ledger 保持 1,171 identities/3,233 occurrences，零新增债务。
- [x] `[SCOPE]` 本切片不迁移 IndexHeader/IndexFile/IndexService 或最终 compatibility ledger；它们继续由
  M06-05e～g 承接。未修改 manifest、feature、runtime ownership、typed-error、observability、持久格式/路径或默认配置。
  顶层统计保持 32 已完成、1 进行中、49 未开始，即仍有 50 个尚未完成。

## M06-05e Index header, slot, and entry codec extraction evidence

- [x] `[DEV/OWNER]` 新增 Local `index::codec`，唯一拥有 canonical 40B `IndexHeaderRecord`、4B `IndexSlot`、
  20B `IndexEntry`、header 字段偏移、空链 sentinel 与 checked file/slot/entry layout；所有持久字段继续采用
  Java-compatible big-endian，header decode 继续把非正 `index_count` 归一为 1。
- [x] `[ADAPTER/COMPAT]` Store `IndexHeader` 继续拥有 mapped-file 与原子状态，只通过 Local record 执行整头
  load/flush，并精确 re-export 旧 `INDEX_HEADER_SIZE` 路径；`IndexFile` 继续拥有 mmap/I/O、hash、time-diff、
  put/query traversal 与日志，但 slot/entry 读写和绝对位置全部委托 Local。文件尺寸错误种类与消息、public path、
  默认 5M/20M 容量、写入顺序、链式冲突语义及磁盘字节保持不变。
- [x] `[TEST/CONTRACT]` Local exact 40B/20B golden、signed round-trip、short-input、legacy index-count 与
  zero/overflow layout 5/5；Store Index 原回归 22/22；新增 M06 Index owner/adapter source contract 1/1，锁定
  Local 唯一 codec/layout owner，并禁止 Store 恢复手写 slot/entry decoder 或布局常量。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency baseline、
  ArcMut final guard、AGENTS routing 与 diff check 通过。切片未触及 `ArcMut` occurrence，ledger 保持
  1,171 identities/3,233 occurrences，零 relocation approval、零新增债务；未触发 runtime、typed-error、
  observability 或 RocksDB 专项门禁。
- [x] `[SCOPE]` 本切片不迁移 IndexFile hash/time-diff/put/query driver、IndexService lifecycle/query/dispatch root
  或最终 compatibility ledger；它们继续由 M06-05f～g 承接。未修改 manifest、feature、持久路径或默认配置。
  顶层统计保持 32 已完成、1 进行中、49 未开始，即仍有 50 个尚未完成。

## M06-05f IndexFile put/query driver extraction evidence

- [x] `[DEV/OWNER]` 新增 Local `index::file`，唯一拥有 `IndexFileSnapshot`、put/query outcome、header update event、
  capacity gate、slot 链头归一化、time-diff clamp、hash 归一化、collision-chain traversal、time/range filter、
  result limit 与成功写入后的 header 更新顺序。driver 只依赖固定长度 read、byte write 和 header event callback。
- [x] `[ADAPTER/COMPAT]` Store `IndexFile` 仅投影 snapshot，提供固定 4B/20B mmap read、byte write、原子 header
  update、shared Java hasher 与日志 adapter；public `put_key`/`select_phy_offset`/`index_key_hash_method`/
  `is_time_matched` 路径和返回语义未变，entry→slot→begin/hash-count/index-count/end 更新顺序保持不变。
- [x] `[FIX/COMPAT]` 固定长度 read adapter 现在按 `MappedFile::get_slice(position, size)` 合同传入 4 或 20；旧查询把
  第二参数误作 end position，导致位于索引文件后半段的合法 entry 被判越界。新增 1-slot/10-entry 回归在第 8 条链记录
  位于文件后半段时验证 8/8 offsets 均可查询，未改变持久字节、链布局或既有文件兼容性。
- [x] `[TEST/CONTRACT]` Local put/query driver 6/6；Local codec/layout 5/5；Store Index 回归扩展为 23/23；
  M06 Index owner/adapter source contract 1/1，锁定 Local driver、Store fixed-size adapter，并禁止 Store 恢复
  time-diff、hash edge、chain traversal 或手写 entry/slot driver。
- [x] `[REV]` Local/Store all-target/all-feature strict Clippy、workspace fmt、architecture dependency baseline、
  ArcMut final guard、AGENTS routing 与 diff check 通过。切片未触及 `ArcMut` occurrence，ledger 保持
  1,171 identities/3,233 occurrences，零新增债务；未触发 runtime、typed-error、observability 或 RocksDB 专项门禁。
- [x] `[SCOPE]` 本切片不迁移 IndexService lifecycle/load/query/build/dispatch root，也不冻结最终 compatibility ledger；
  它们由 M06-05g 承接。未修改 manifest、feature、持久路径、容量默认值或 service lifecycle。顶层统计保持
  32 已完成、1 进行中、49 未开始，即仍有 50 个尚未完成。

## M06-05g IndexService root and PR-M06-05 parent closeout evidence

- [x] `[DEV/OWNER]` Local `index::service` 新增 canonical `IndexServiceRoot<A>`、`IndexServiceFile` port、
  `QueryOffsetResult`、load/safe-offset/expiration/shutdown/destroy、tail reuse/create/retry/flush、跨文件 query、
  dispatch preflight、unique→normal→tag key traversal 与 key builder；Local `index::dispatch` 新增 feature gate/progress root。
- [x] `[FACADE/ADAPTER]` Store `IndexService` 收敛为单字段 `IndexServiceRoot<IndexServiceAdapter>` facade，旧 public
  方法通过 `Deref/DerefMut` 保持可用；adapter 仅拥有目录 I/O、`RwLock<Vec<Arc<IndexFile>>>`、config/checkpoint/
  error flag、shared Java hasher、等待与 detached flush、日志和 `DispatchRequest`/MessageConst 投影。
  `CommitLogDispatcherBuildIndex` 同样收敛为单字段 `IndexDispatchRoot<IndexDispatchAdapter>`；旧
  `index::query_offset_result::QueryOffsetResult` 精确 re-export Local owner。
- [x] `[COMPAT]` load 对不安全文件的删除、legacy checkpoint safe-offset 恢复、过期文件始终保留 newest、3 次 create
  retry/1 秒等待、full-file rollover/async flush fallback、query newest-first/metadata/limit、rollback/empty/old-offset gate、
  unique/normal/tag 次序和 tag failure 后 safe-offset 语义均未改变。40B/4B/20B/46B/CQExt 磁盘格式、目录、默认容量、
  public path 与 feature 未改变。
- [x] `[TEST]` Local Index service/dispatch root 7/7、file driver 6/6、codec/layout 5/5；Store Index service/file 23/23、
  dispatcher gate/progress 2/2；Local all-feature unit/integration/doctest 全绿，Store all-feature lib 544/544。
  M06 CQ/Flush/Index owner contract 3/3、Store API contract 8/8、StoreLocal ledger/feature/forbidden-edge impacted contract 3/3。
- [x] `[VALIDATION NOTE]` monolithic `test_m06_*` discover 与单独 160-test StoreLocal mutation suite 分别在 304 秒和
  604 秒工具窗口超时，均未形成结果且未计为通过；受本 PR 影响的 Index、ledger、feature/forbidden-edge 契约已拆分重跑通过，
  M06-04 冻结快照上的既有 160/160 历史证据未被改写。
- [x] `[REV]` Local/Store strict Clippy、root workspace `--no-deps --all-targets --all-features -D warnings` Clippy、
  workspace fmt、architecture dependency、ArcMut zero-growth、AGENTS routing、enforcing runtime audit 与 diff check 全部通过。
  本切片未触及 `ArcMut` occurrence，ledger 保持 1,171 identities/3,233 occurrences，零新增债务。
- [x] `[LEDGER]` `06-storage-local-compatibility-ledger.md` 冻结 M06-03/04/05 owner 快照，新增 CQ/Index canonical
  owner、Store-only port、removal milestone、回滚与当前验证证据；对应 mutation contract 通过。
- [x] `[MAIN/ROLLBACK]` `git fetch origin main` 后候选分支相对 `origin/main` 为 0 behind；发布分支在不改变最终树内容的
  前提下整理为一个 Issue-linked commit。可整体 revert PR-M06-05，但不得恢复 Local/Store 双 owner；已写 CQ/Index 文件无需数据迁移。
- [x] `[INVENTORY]` PR-M06-05 父项关闭；82 个顶层工作包更新为 33 已完成、0 进行中、49 未开始，即 49 个尚未完成。
  M06 整体仍进行中且 Exit Checklist 保持未完成；下一工作包为 PR-M06-06。

## M06-06 HA, replication, and transfer boundary extraction evidence

- [x] `[ARCH/SCOPE]` 冻结 transfer header/offset report、replica frame 连续性、confirm-offset clamp、ACK 条件、
  sync-state set、epoch transition、flow-control budget 与 partial-write 顺序；本包只迁移 backend-neutral owner，
  不改变 CommitLog/CQ/Index 持久格式、HA wire 字节、默认 batch/flow 参数或 Rocks/Tiered 组合。
- [x] `[DEV/OWNER]` Local 新增 canonical `transfer::{batch,error,planner,segment}` 与
  `ha::{wire,flow,replication,transfer_engine,transfer_metrics}`；`ReplicationStateRoot`/`ReplicationProgress`
  统一拥有 role/master、epoch、sync-state set、replica ACK snapshot、confirm-offset 与单调进度，transfer engine
  统一拥有 bytes/vectored/sendfile partial-write driver 和 metrics。
- [x] `[DEV/ADAPTER]` Store 旧 transfer/HA path 收敛为精确 re-export 或窄 adapter；socket/network、controller
  Remoting DTO、`LocalFileMessageStore`/CommitLog append、配置投影、日志以及 `ServiceManager`/`ServiceContext`/
  `TaskGroup` 生命周期仍留在 Store。`DefaultHAClient`、`DefaultHAConnection`、`AutoSwitchHAService`、
  `DefaultHAService` 与 `GroupTransferService` 均委托 Local wire/state/progress/ACK 算法，不复制第二 owner。
- [x] `[TEST]` `rocketmq-store-local --all-features` 的 126 个 unit tests、全部 integration/doctest，Local HA
  planner/partial-write boundary 2/2，Store focused HA/transfer 22/22，以及 Store all-feature lib 531/531 全部通过；
  M06 HA source contract 2/2 锁定唯一 Local owner、Store facade、依赖禁边与 runtime adapter 边界。
- [x] `[RUNTIME/COMPAT]` enforcing runtime audit 通过；Local HA 不含 `tokio::spawn`、Remoting/controller DTO、
  `LocalFileMessageStore` 或第二 WAL，生产 background work 继续由既有 `ServiceContext`/`TaskGroup` 拥有。
  旧 public module/type identity、wire/storage bytes、feature/default 与 shutdown ownership 保持兼容。
- [x] `[ARC/REV]` Local/Store strict all-target/all-feature Clippy、architecture dependency guard、workspace fmt/
  Clippy、AGENTS routing 与 diff check 通过。13 个既有 occurrence 因 owner 搬迁发生同 item 一对一 fingerprint
  relocation，按 ADR-013 精确批准；`current_milestone` 保持 M05，ledger 仍为 1,171 identities/3,233 occurrences，
  零新增共享可变状态债务。
- [x] `[VALIDATION NOTE]` typed-error hygiene 初次发现本包新增的 wire error 字符串化映射；已改为
  `HAClientError::Wire` typed source 并复扫消除。本门禁仍非零退出，只复现未改动路径的 Broker source-stringification
  1 项、MCP anyhow 8 项和缺失治理文档 2 项；`git diff --name-only main` 与这些路径交集为空，因此不计为本包通过，
  也没有用 allowlist 掩盖。
- [x] `[LEDGER]` `06-storage-local-compatibility-ledger.md` 冻结 M06-06 canonical owner、Store-only network/
  runtime/effect ports、removal milestone、兼容规则和验证快照；M06-07/08 的 Timer/POP 与 store composition 未提前关闭。
- [x] `[MAIN/ROLLBACK]` 候选分支从最新 main 创建；可整体 revert PR-M06-06，使 Store composition 指回旧
  HA/transfer adapter，但不得恢复 Local/Store 双 owner，也不回滚 CommitLog/flush/CQ/Index owner；无磁盘数据迁移。
- [x] `[INVENTORY]` PR-M06-06 父项关闭；82 个顶层工作包更新为 34 已完成、0 进行中、48 未开始，即
  48 个尚未完成。M06 整体仍进行中且 Exit Checklist 保持未完成；下一工作包为 PR-M06-07。

## M06-07 Timer, POP, and Local Services boundary extraction evidence

- [x] `[ARCH/SCOPE]` 盘点 Timer、POP、cold-data、stats、hook/filter 的 owner、依赖和调用点；冻结 40B timer-log、
  56B checkpoint、wheel slot、POP ACK/BatchACK/Checkpoint Serde、TPS/延迟分桶、hook 注册顺序与冷数据默认行为。
  本包不改变 CommitLog/CQ/Index 持久格式、Timer 默认参数、POP revive 语义或 LocalFileMessageStore composition。
- [x] `[DEV/OWNER]` Local canonical 拥有 `timer::{slot,timer_log,timer_wheel,checkpoint,metrics,service}`、
  `pop::{ack_msg,batch_ack_msg,pop_check_point}`、`services::cold_data_check`、`stats::{stats_type,StoreStatsState}`、
  `filter` 与泛型 `HookRegistry`。Timer schedule/recovery/backlog/TPS 和 Store TPS/延迟/运行时统计不再由 facade 复制。
- [x] `[DEV/ADAPTER]` Store Timer 仅保留 `MessageExt` 投影、CommitLog/CQ 副作用、`MessageStoreConfig`、磁盘文件、
  `DataVersion`/`ConfigManager` 与 TaskGroup scheduler；`StoreStatsService` 仅保留 `BrokerIdentity`、TaskGroup 和 start/stop
  lifecycle，并通过 `Deref` 暴露 Local state。旧 POP/filter/stats/timer/cold-data 深路径精确 re-export；真实 put-message
  hook 列表改由 Local registry 持有，legacy trait/getter 不变。
- [x] `[TEST]` 新增 M06-07 source contract 4/4；Local all-feature 176 个 unit tests及全部 integration/doctest通过；
  Store Timer 30/30、stats lifecycle 3/3、timer restart integration 3/3、Store all-feature lib 483/483 通过。
  cold-data 当前 page-cache/cold-area 默认值、POP Serde/checkpoint/revive fields、timer recovery/expiry/roll 和 hook 顺序均有回归覆盖；
  Local `--no-default-features` check 通过。Store no-default 仍复现已记录的空 `GenericMessageStore` baseline，不属于 R0 承诺且未计为通过。
- [x] `[RUNTIME/DEPENDENCY]` enforcing runtime audit、architecture dependency guard 35/35、AGENTS routing 与 Local
  manifest/forbidden-edge impacted contract 2/2 通过。Local 不含 Store/Common/Remoting/Broker/DataVersion、
  `LocalFileMessageStore` 或 detached `tokio::spawn`；既有 TaskGroup owner 和 shutdown 时序保持在 Store adapter。
- [x] `[REV]` workspace fmt、diff check、Local/Store 与 workspace all-target/all-feature strict Clippy 通过。完整既有 StoreLocal mutation
  suite 超过 240 秒工具窗口，未形成结果且未计为通过；与本包直接相关的 workspace feature ownership 和 Local
  forbidden-edge 两项已拆分重跑全绿，新增 M06-07 contract 4/4 全绿。hook 字段替换和 Timer import 抽取引起 3 条
  既有 ArcMut fingerprint 一对一 relocation，按 ADR-013 精确批准并完成 monotonic promotion/compare；账本仍为
  1,171 identities/3,233 occurrences，零新增共享可变状态债务，guard 与 24 项 fixture 全绿。
- [x] `[LEDGER/ROLLBACK]` compatibility ledger 冻结 Timer/POP/services/stats/filter/hook owner、Store-only protocol/
  runtime/effect ports与旧路径删除条件。可整体 revert PR-M06-07，或按 Timer、POP、services delegation 回切；不得恢复
  Local/Store 双 owner，既有 timer/POP 持久数据无需迁移。
- [x] `[INVENTORY]` PR-M06-07 父项关闭；82 个顶层工作包更新为 35 已完成、0 进行中、47 未开始，即
  47 个尚未完成。M06 整体仍进行中且 Exit Checklist 保持未完成；下一工作包为 PR-M06-08。

## M06-08 LocalFileMessageStore composition and config evidence

- [x] `[ARCH/SCOPE]` 冻结 Store 公共 `LocalFileMessageStore` 类型路径、126 方法 legacy `MessageStore` trait、
  `MessageStoreConfig` Serde/default/camelCase/snake_case alias、CommitLog 多路径和现有数据目录；本包不改变消息、
  lifecycle、query/reput/cleanup 行为，不修改 wire/storage bytes，也不触碰 RocksDB/Tiered owner。
- [x] `[DEV/OWNER]` Local canonical 新增 `message_store::{lifecycle,query,reput,cleanup}` 和
  `message_store::local_file_message_store::LocalStoreComposition`；lifecycle atomic state、index safety/degradation、
  reput end-offset/availability/behind 与 disk-pressure/manual-delete policy 不再由 Store 重复定义。
- [x] `[DEV/CONFIG]` Local `config::backend::LocalBackendConfig` 为 immutable normalized backend config，拥有规范化
  root/CommitLog paths、recovery、query、reput、cleanup 与 I/O hint 值；Store `MessageStoreConfig` 继续作为唯一 legacy
  Serde envelope，并通过 `normalized_local_backend_config()` 完成默认值、alias、多路径和 ratio clamp 投影。
- [x] `[DEV/ADAPTER]` Store public facade 保持原路径和构造签名，组合 Local root，仅保留 Broker/MessageExt、CommitLog/CQ/
  Index、文件锁、TaskGroup、checkpoint、日志和 RunningFlags 等具体副作用 adapter。background-index、reput 与 cleanup
  adapter 只消费 Local policy；没有把 Common、Remoting、Broker、ArcMut 或 detached task 带入 Local。
- [x] `[TEST]` public-path doctest 和 source contract 锁定 legacy facade/Local root；Local 184 unit tests及全部
  integration/doctest、Store LocalFileMessageStore focused 83/83、config 35/35 和 composition compile/projection 回归通过。
  load/start/shutdown/destroy、recovery lifecycle、query safety、reput committed/uncommitted 和 cleanup 阈值/手工重试均被覆盖。
- [x] `[RUNTIME/DEPENDENCY]` Local composition 模块保持 runtime-neutral，manifest/source 不含 Store/Common/Remoting/Broker、
  `MessageExt`、ArcMut、ScheduledTaskGroup 或 `tokio::spawn`；既有 Store TaskGroup、blocking executor 与 shutdown 顺序不变。
- [x] `[REV]` 新增 Local composition/config 模块均低于 800 行并保持单向依赖。Store 9K legacy 文件的物理搬移会把
  既有 ArcMut occurrence 变成新 path identity，违反 ADR-013 monotonic guard，因此不以扩大台账换取行数指标；126 方法
  compatibility trait adapter 与同文件 tests 保留到 PR-M06-12 capability 收敛，新增 lifecycle/query/reput/cleanup 算法
  均位于 Local，Store 仅保留 effect orchestration。
- [x] `[ARC/COMPAT]` 14 条既有 occurrence 因 composition 字段和 policy 参数发生同 item 一对一 fingerprint relocation，
  已按 ADR-013 精确批准并完成 monotonic promotion/compare；ledger 保持 1,171 identities/3,233 occurrences，零新增债务。
- [x] `[LEDGER/ROLLBACK]` compatibility ledger 冻结 Local composition/config owner、Store-only effect ports、R0 public path、
  removal milestone 与验证快照。可整体 revert 本 PR 使 facade 恢复直接状态/决策；旧 config envelope、数据目录和磁盘内容
  无需迁移，且不得回滚 M06-03～07 已完成 owner。
- [x] `[INVENTORY]` PR-M06-08 父项关闭；82 个顶层工作包更新为 36 已完成、0 进行中、46 未开始，即
  46 个尚未完成。M06 整体仍进行中且 Exit Checklist 保持未完成；下一工作包为 PR-M06-09。
