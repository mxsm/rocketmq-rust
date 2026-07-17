# M10-03 CQ、RocksDB 与 Tiered 读路径优化实施证据

## 结果

M10-03 在不改变 CommitLog、ConsumeQueue 或 RocksDB 持久格式的前提下收口三条读路径：Local CQ
从 mmap 借用切片解析，RocksDB CQ 以一次范围扫描返回 typed value，Tiered 以全局 byte budget 管理
generation-aware block cache 并合并相邻读取。该包完成后架构盘点为 **62/82**，剩余 20 个工作包；
M10 剩余 2 个，下一包为 PR-M10-04。

## 可追溯性

| 字段 | 值 |
|---|---|
| 工作包 | `PR-M10-03` |
| Issue | [#8264](https://github.com/mxsm/rocketmq-rust/issues/8264) |
| 分支 | `mxsm/architecture-refactor-read-path-optimization` |
| Main 基线 | `313b73671ec0cac57e853f67bbfed23b5304070a` |
| 冻结代码候选 | `e3ef67282f420334a4f2bccc7fa01c81facbd60a` |
| 运行期证据 | `target/architecture-refactor/M10/read-path-8264/` |

## 目标与合同结论

### Local ConsumeQueue

- `ConsumeQueueIterator` 直接调用 `ConsumeQueueRecord::decode(&mmap[start..end])`；20-byte 单元解析不创建
  `Bytes`、`Vec` 或其他临时堆 buffer，因此每单元解析的临时 heap allocation 为 0。
- `iterate_from_with_count` 现在实际执行有界请求，`count <= 0` 返回空迭代器，不再退化为读取整个映射区。
- `LocalFileMessageStore::get_message` 按 `min(max_msg_nums, max_msgs_num_batch)` 预分配结果，替代固定 100 项容量；
  测试同时固定请求容量和两条真实派发消息的读取语义。

### RocksDB ConsumeQueue

- CQ key 保持 Java-compatible big-endian 布局；`[start, end)` 只执行一次 native range scan，并逐项校验
  精确连续 key，遇到第一个 hole 停止。
- Store root 直接消费 `ConsumeQueueValue`，删除 legacy typed→Bytes→typed round trip；旧 `range_query`
  兼容 facade 保留，新的 typed API 为 additive。
- 实测完整拉取为 2 次 offset boundary point read 加 1 次 CQ range scan，总 native calls=3；CQ range
  本身为 scan=1、point read=0。

### Tiered block cache

- `TieredFlatFileStore` 为所有 topic/queue 共享一个 cache，默认 retained-byte 硬上限 64 MiB；CQ block
  为 64 KiB，CommitLog block 为 1 MiB，按 byte LRU/TTL 淘汰。
- cache key 包含 segment path、metadata `create_timestamp` generation、segment type 和 block offset；删除
  segment 时按 path 失效所有 generation，store destroy 时清空。
- 32 条连续消息冷拉精确执行 2 次 provider read（一个 CQ block、一个 CommitLog block），满足 ≤4；
  同范围热拉为 0 次。

## 正确性、边界与 lease 审查

- 旧 `ConsumeQueueUnit::decode(Bytes)` 入口保留，并委托新增借用解码；owned 与 borrowed 入口用同一
  golden unit 验证，未产生 source compatibility 删除。
- cache 命中返回 `Bytes::slice`，活跃 pull 可在 cache 淘汰后继续持有 backing block；这属于读取 lease，
  不计入 cache retained bytes。活跃 lease 仍受 pull message count 和 byte size 上限约束，不形成 cache
  retained-byte 逃逸。
- 同 generation 的 append-only segment 若在已缓存 block 后增长，required length 会使短 block miss 并重新
  读取；旧 generation 不会命中新 generation。
- 本包不改变消息 body、CQ、CommitLog、Rocks key/value 编码，也不改变 Broker ack、durable watermark、
  cursor 或 retry ledger 语义。

## 公共 API 审查

31 个 Rustdoc target 全部刷新。初始检查只报告三个 `surface-changed`：

| crate | public path count | public path hash | 审查结论 |
|---|---:|---|---|
| `rocketmq-store` | 382 | 不变 | 内部结果容量与借用 iterator 实现变化 |
| `rocketmq-store-rocksdb` | 165 | 不变 | additive typed range API 与内部直接消费 |
| `rocketmq-tieredstore` | 116 | 不变 | additive cache byte budget/借用 decode，旧入口保留 |

没有公共路径删除、重命名或 breaking diff；审核后基线绑定冻结代码候选。最终提交态重新刷新并要求
`PUBLIC_API_SNAPSHOT_OK packages=31 differences=0`。

## 验证记录

| 命令/门禁 | 结果 |
|---|---|
| Local CQ/结果容量 focused tests | 通过；borrowed iterator 1/1，真实 get 1/1 |
| Tiered package | 60 个 library、1 个 POSIX、7 个 cursor/retry 集成测试通过 |
| Store library | 480/480 通过 |
| Store/Broker RocksDB strict Clippy 与专项矩阵 | 通过；foundation 82、semantics 9、Broker Rocks 20、POP 4 |
| affected-crate all-target/all-feature strict Clippy | 通过 |
| dependency fixtures/target/baseline 与 release guard | 通过：target 35/35，dev-only 3/3 |
| ArcMut fixtures/direct guard | 通过；零新增、零 baseline 增长 |
| runtime enforcing audit | 通过；无新增 task/thread/runtime owner |
| typed-error hygiene 与 AGENTS routing | 通过 |
| MCP consumer check/test/streamable-http strict Clippy/doc | 通过；73 library，兼容与 integration 语料通过 |
| 根 `cargo fmt --all -- --check` 与 workspace strict Clippy | 通过 |
| public API snapshot | 最终 31/31，differences=0 |
| `git diff --check` | 通过 |

### 已披露的基线失败

`cargo test -p rocketmq-store` **不记录为通过**。单独复跑
`file_store_vs_rocksdb_behavior_parity_after_restart` 仍失败：fixture 配置 `StoreType::RocksDB`，但构造的仍是
`LocalFileMessageStore`，伪 Rocks 路径重启后没有逻辑队列。`git diff --exit-code origin/main --
rocketmq-store/tests/commitlog_recovery_tests.rs` 返回零并输出 `BASELINE_FIXTURE_UNCHANGED`，证明测试和工厂未被
M10-03 修改。真实 RocksDB foundation/semantics 与 Broker 专项全部通过。

## 审查与回滚

`[REV]` 结论：持久格式、body lease、generation 隔离和 first-hole 语义保持；cache retained bytes 有全局
硬上限；RocksDB 无额外 decode round trip；M10-03 最终 material finding 为 0。

回滚时先关闭 Tiered read-ahead 配置并清空 cache，Rocks/Local 切回已通过相同读取正确性、generation 与
durability corpus 的上一 read adapter。若上一实现没有同等证据，则停止受影响读取并保留 cache/generation
现场，不做未验证降级；回滚不得改变或重写现有 CQ、CommitLog、Rocks 持久数据。
