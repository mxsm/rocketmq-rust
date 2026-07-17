# M10：耐久派生引擎与可量化性能

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 3：性能、耐久引擎与云原生 |
| 状态 | 实施中（PR-M10-03 已完成，下一工作包 PR-M10-04） |
| 预计周期 | 5–8 周 |
| 工作包 | WP14 `tiered-cursor`；延续 WP02、WP11–WP13 |
| 前置条件 | 32-package Gate 通过；CommitLog/receipt/progress 和 Local/Rocks golden 稳定 |
| 可并行项 | CQ/Rocks read、Tiered、Compaction、benchmark harness 可并行；持久 cursor/format 变更必须串行审核 |
| 完成后解锁 | M12；与 M11 可部分并行 |

## 目标

- 以 CommitLog 作为唯一 WAL/outbox，让 CQ、Index、RocksDB、Tiered、Compaction 各自持久连续 cursor/watermark。
- 消除派生事件丢失、裸 CommitLog 回退和无界 retry/queue 状态。
- 通过 byte backpressure、零/少复制和 I/O 合并改善 RSS、p99 与 amplification。
- 建立可重复的正确性、故障和性能 Gate，关键指标相对固定基线不恶化超过 5%。

## 非目标

- 不创建第二消息日志，不让派生引擎参与主写 ack。
- 不改变现有持久格式；新增 metadata/generation 必须 versioned、独立目录、可回滚。
- 不为追求 benchmark 假设牺牲 durability、boundedness 或兼容性。

## 入口条件

- [x] `[ARCH]` 冻结 source_epoch、physical_offset、cursor、retry ledger、generation lease 和 Recovering 语义。
- [x] `[TEST]` 固定硬件/profile、golden log、kill/restart、磁盘/Provider 故障和统计方法。
- [x] `[DEV]` 确认 store-local/rocks/tiered 目标文件无用户修改重叠。
- [x] `[HUMAN]` 批准新增持久 metadata 的 version、原子切换和上一 generation 回滚策略。

## 交付物

| 类型 | 交付物 |
|---|---|
| WAL outbox | 各派生引擎独立 durable cursor/watermark，幂等 replay |
| Tiered | 持久 retry ledger、队列 worker、背压、cursor、provider durability capability |
| Reads | CQ slice parse、Rocks range/multi-get、Tiered block cache/read coalescing |
| Compaction | versioned generation、CURRENT 原子切换、reader lease、Recovering 状态 |
| Resource | count+byte budget、buffer pool、WAL pin 上限和 readiness/alert |
| Evidence | baseline/candidate、soak、fault、kill/restart、format golden、回滚演练 |

## PR 级开发步骤

### PR-M10-01：派生 cursor 合同和 replay harness

- [x] `[ARCH]` 定义 per-engine cursor 的连续推进、durable commit、source epoch 和幂等键。
- [x] `[DEV]` 在 store-api/owner 中增加 versioned progress，不改变 AppendReceipt 主写语义。
- [x] `[TEST]` 用 golden CommitLog 重放，覆盖崩溃点、重复事件、尾部损坏、cursor 损坏和升级。
- [x] `[REV]` 检查每个派生引擎只写自己的 metadata，不写 payload/WAL。
- [x] 回滚点：停止派生 reader、设置 `readiness=false`，冻结最后已提交 cursor/ledger并 pin 所需 WAL；修复后从该 checkpoint 幂等重放。只有上一实现先通过同一 durability corpus，才允许受控切换。

完成证据：[`10-derived-cursor-replay-evidence.md`](10-derived-cursor-replay-evidence.md)。候选提交
`1f2a7bb4ae5d6adf271fc34e63c60c956c22606e` 固化 32-byte versioned checkpoint、连续 cursor、幂等 replay
与崩溃/脏尾/损坏/升级 corpus；真实 Tiered persistence 与 retry/backpressure 继续由 PR-M10-02 实现。

### PR-M10-02：Tiered cursor、retry ledger 与背压

- [x] `[DEV]` 顺序读取 CommitLog；channel 满时暂停 reader，不丢事件。
- [x] `[DEV]` 失败项先原子持久 `(source_epoch, offset, length, topic, queue, retry_state)`，再允许全局 cursor 越过。
- [x] `[DEV]` ledger 不复制 payload，以 offset tuple 幂等；按 count/bytes/age 硬限制并 pin 最小 WAL segment。
- [x] `[DEV]` 到上限时停止 reader、readiness=false、告警并施加背压；分区 worker 隔离/退避。
- [x] `[TEST]` 覆盖 provider timeout/partial write/restart/重复提交/ledger 满/WAL pin 回收。
- [x] `[REV]` 检查 cursor/retry 不参与 Broker ack，无无界“完成但未提交”内存。
- [x] 回滚点：停止对应 topic/queue reader、设置 `readiness=false`，冻结已提交 cursor 与 retry ledger并保留/pin WAL；修复后从已验证 checkpoint 重放。不得切换任何未通过同一 cursor/retry/kill-restart corpus 的实现。

完成证据：[`10-tiered-cursor-retry-evidence.md`](10-tiered-cursor-retry-evidence.md)。候选提交
`1a5463b143f33a8246d32d82abd05ddacf3b14c6` 将 payload-free retry ledger、原子 cursor、count/bytes/age
硬上限、WAL pin、分条退避和上游背压接入真实 Tiered reput；61/82 已完成，下一工作包为 PR-M10-03。

### PR-M10-03：CQ、Rocks 与 Tiered 读取优化

- [x] `[DEV]` CQ 在借用 slice 上解析 20B unit，结果按请求上限预分配。
- [x] `[DEV]` Rocks 使用 prefix/range iterator 或 multi-get，直接返回 typed value。
- [x] `[DEV]` Tiered 使用按 bytes 限制的 generation-aware block cache并合并相邻 ranges。
- [x] `[TEST]` 验证 CQ allocation=0、Rocks native calls≤3、cold pull provider calls≤4、warm=0 的假设；未达假设不影响正确性结论。
- [x] `[REV]` 检查 body lease 生命周期、cache key generation 和无额外 decode round-trip。
- [x] 回滚点：读取优化可切换到已通过同一读取正确性、generation 和 durability corpus 的上一 read adapter；否则停止受影响读取、保留 cache/generation 证据，不做未验证降级。

完成证据：[`10-read-path-optimization-evidence.md`](10-read-path-optimization-evidence.md)。候选提交
`e3ef67282f420334a4f2bccc7fa01c81facbd60a` 保持 Local/Rocks/Tiered 持久格式不变；CQ 单元借用解析的
每单元临时 allocation 为 0，Rocks 完整拉取为 2 次点读加 1 次 range scan，Tiered 32 条冷拉为 2 次
provider read、热拉为 0。62/82 已完成，下一工作包为 PR-M10-04。

### PR-M10-04：Index/Compaction generation

- [ ] `[DEV]` 构建 `gen-N.tmp`，校验 CRC/条数/边界，sync 后原子切换 CURRENT，延迟删除旧 generation。
- [ ] `[DEV]` reader 持 generation lease；旧 generation 在最后 reader 退出后删除。
- [ ] `[DEV]` compaction 只保 key→latest offset，未追平时返回 Recovering 或经过证明的旧 generation+delta，不裸回退 CommitLog。
- [ ] `[TEST]` 在 build/sync/rename/CURRENT/cleanup 每个故障点 kill/restart，并验证旧 key 不重新可见。
- [ ] `[REV]` 检查目录 fsync、lease/epoch、version兼容和回滚到上一 generation。
- [ ] 回滚点：只有上一 generation 通过同一 CRC/边界/kill-restart/durability corpus 时，才原子将 CURRENT 指回；否则保持 Recovering/readiness=false 并修复当前 generation。

### PR-M10-05：Benchmark、soak 与性能 Gate

- [ ] `[TEST]` 固定 Local append/SyncFlush/Local pull/Rocks pull/Tiered append/pull/connection soak/overload profile。
- [ ] `[DEV]` 实现计划中的 `architecture_performance_guard.py`，比较吞吐、p99、RSS、allocation、I/O amplification并保存元数据。
- [ ] `[TEST]` 重复采样并计算噪声；通用 Gate 为关键指标不恶化超过 5%，提升数字作为 hypothesis 单独报告。
- [ ] `[REV]` 检查报告含硬件、内核、文件系统、profile、feature、message size、TLS/活跃度和原始数据 hash。
- [ ] `[HUMAN]` 仅在正确性先通过后批准性能例外；例外有 owner/期限/回退配置。

## 公共兼容面

- CommitLog/CQ/Index/Rocks 既有格式不变；新 cursor/retry/generation 使用独立 versioned metadata。
- 主写 AppendReceipt/ack 不等待 Tiered/Compaction；派生进度通过 DerivedProgress/StoreHealth。
- Recovering/overload 使用稳定 typed error 映射，不静默返回不完整/旧数据。
- shadow build/read 与 topic 切换必须可撤销并保留上一 generation。
- Tiered/派生回滚默认是停止 reader、readiness=false、冻结 cursor/ledger、pin WAL并从 checkpoint 重放；“上一实现”只有通过同一 durability corpus 后才是合法切换目标。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-store
cargo test -p rocketmq-tieredstore
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
cargo test -p rocketmq-store-local
cargo test -p rocketmq-store-rocksdb
cargo test -p rocketmq-tieredstore tiered_cursor
cargo test -p rocketmq-tieredstore retry_ledger
python scripts/architecture_performance_guard.py --baseline <baseline.json> --candidate <candidate.json>
python scripts/architecture_dependency_guard.py --mode target
```

性能 guard 与命名测试仅在实际落地后执行；kind/cloud guard属于 M11。

## 回滚触发器

- 已确认消息 crash/restart 后不可见，或派生出现 hole/重复可见。
- cursor 越过未持久 retry、ledger/queue/WAL pin 无界、裸 CommitLog 回退暴露旧 key。
- generation 无法原子回切或持久格式发生未版本化改变。
- 正确性通过但关键吞吐/p99/RSS 超过 5% 回归且无获批例外。

立即停止受影响 backend/topic 的派生 reader并设置 `readiness=false`，冻结已提交 cursor/ledger，pin/保留所需 WAL 和新 metadata；修复后从该 checkpoint 幂等追赶。只有通过同一 durability corpus 的上一实现或上一 generation 才允许切换；durability 失败升级 `[HUMAN]`。

## Exit Checklist

- [ ] `[TEST]` SyncFlush ack 消息 crash/restart 后 100% 可见。
- [ ] `[TEST]` 各派生 replay 无 hole/重复，cursor/retry 故障可恢复。
- [ ] `[REV]` CommitLog 是唯一 WAL，派生 metadata 不含 payload。
- [ ] `[TEST]` Tiered 满载有界且不丢事件，readiness/alert 正确。
- [ ] `[TEST]` compaction/index 所有 kill point 可恢复或回到上一 generation。
- [ ] `[REV]` 不存在裸 CommitLog 语义回退。
- [ ] `[TEST]` Tiered/派生停止、冻结 cursor、pin WAL、checkpoint 重放和“仅切换已验证上一实现”的回滚演练通过。
- [ ] `[TEST]` 性能 Gate和报告元数据完整。
- [ ] `[HUMAN]` 持久格式、性能例外和 M10 Gate 已签署。

## 交接物

- 向 M11 交付 durability/lag/readiness SLI、磁盘压力和 drain/flush 故障场景。
- 向 M12 交付 versioned DerivedProgress/StoreHealth 证据，不暴露 WAL 或 payload。
