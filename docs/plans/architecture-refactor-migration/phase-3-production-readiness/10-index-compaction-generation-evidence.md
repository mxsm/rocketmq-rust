# M10-04 Index/Compaction generation 实施证据

## 结果

M10-04 将 Tiered Index 与 Local Compaction 的发布路径收口为 versioned generation。两条路径都先构建
`gen-N.tmp`，校验 CRC、条数与 offset/time 边界，完成数据和目录持久化后再原子切换 `CURRENT`；旧代由
reader lease 延迟回收。Compaction generation 会复制 live record，重启读取不依赖原 CommitLog payload；
未完成 WAL replay 时保持 `Recovering`，不存在裸 CommitLog 扫描回退。该包完成后架构盘点为 **63/82**，
剩余 19 个工作包；M10 剩余 1 个，下一包为 PR-M10-05。

## 可追溯性

| 字段 | 值 |
|---|---|
| 工作包 | `PR-M10-04` |
| Issue | [#8266](https://github.com/mxsm/rocketmq-rust/issues/8266) |
| 分支 | `mxsm/architecture-refactor-index-compaction-generation` |
| Main 基线 | `4dfbad5667b6bb5b2c85f39e23d3c7342e210f50` |
| 冻结代码候选 | `92608c2962bc6e3f0e226980eb7bbf92ac162c1f` |
| 运行期证据 | `target/runtime-audit/` |

## Index generation 合同

- `IndexGenerationManager` 使用带 magic/version/CRC 的固定格式 `CURRENT` 与 generation metadata；metadata
  同时记录 entry count、最小/最大时间戳和物理 offset，加载与发布前均重新验证。
- 发布顺序固定为 build → sync → validate → rename → `CURRENT` → cleanup。POSIX 在 rename/replace 后同步
  parent directory；Windows 使用带 write-through 语义的 `MoveFileExW`/`ReplaceFileW`。
- `IndexFileSegment` 读取快照持有 generation `Arc` lease；清理只删除 strong count 为 1 的 retired generation。
  重启会删除 `.tmp`/未引用孤儿代，损坏 current 只回滚到重新校验通过的 previous；瞬态 provider 读错误不触发回滚。
- Provider 新增的 sync、rename、atomic write、prefix list/delete 能力均有默认实现，已有 provider 实现保持
  source compatible；Memory/POSIX/ProviderKind 路径分别补齐实际能力。

## Compaction generation 合同

- delta 仅保存 CommitLog 精确位置；compact 时解析 live record 并复制 payload 到新 generation。发布后内存记录
  只保留 key/queue/source offset 与 generation 文件的 payload position/size，不长期持有完整 payload 或 mmap lease。
- 测试在 compact 后清空 CommitLog resolver，并在不安装 resolver 的新进程视图中重启，仍从 generation 文件读到
  live payload，证明 generation 不是旧 CommitLog 的 offset 索引壳。
- 当前代与 delta 先做 latest-physical-offset 过滤；replay 未达到 durable compaction watermark 时返回稳定的
  `MessageWasRemoving`（内部 `Recovering`），读取失败返回 `OffsetFoundNull`，不会扫描裸 CommitLog 重新暴露旧 key。
- Compaction dispatcher 的恢复起点纳入 generation durable watermark。CURRENT 损坏或 current generation 校验失败时，
  只选择重新验证通过的 previous generation，并继续保持 Recovering 直至 delta replay 完成。
- build/sync/rename/CURRENT/cleanup 五个故障点均执行 kill/restart corpus；cleanup failure 保留已发布 current，
  其余 publication failure 保留上一有效代。reader lease 测试证明活跃读者退出前旧代不会删除。

## 兼容性与债务审查

- 既有 CommitLog、CQ、Index entry、RocksDB key/value 格式不变；新增 generation/CURRENT 位于独立 versioned 路径。
- CommitLog 仍是唯一权威 WAL/outbox；Compaction generation 是可重建的 live-record 派生视图，不参与 append ack，
  也不具备追加日志或 replay source 语义。
- ArcMut guard 的 production occurrence 从 3191 降至 3189；Compaction dispatcher 不再长期持有 CommitLog `ArcMut`。
- 31 个 Rustdoc target 的 public path count/hash 全部不变。生成基线绑定冻结代码候选，并复核为
  `PUBLIC_API_SNAPSHOT_OK packages=31 differences=0`；变化仅为内部 Rustdoc JSON 实现哈希。

## 验证记录

| 命令/门禁 | 结果 |
|---|---|
| Compaction focused tests | 8/8 通过；含五故障点、损坏回滚、reader lease、无 CommitLog payload 重启读取 |
| Store library | 486/486 通过 |
| Tiered package | 66 library、1 POSIX、7 cursor/retry 集成测试通过 |
| Store/Broker RocksDB 专项 | 通过；foundation 82、semantics 9、Broker Rocks 20、POP 4 |
| affected-crate strict Clippy | Store/Tiered all-target/all-feature 通过 |
| MCP consumer | check、73 library、兼容/integration、streamable-http strict Clippy、doc 通过；外部集群 e2e 1 项按设计 ignored |
| dependency/release/ArcMut/AGENTS routing | 通过；target 35/35、dev-only 3/3、32-package release topology、ArcMut 零新增 |
| runtime enforcing audit 与 typed-error hygiene | 通过 |
| 根 `cargo fmt --all -- --check` 与 workspace strict Clippy | 通过；workspace Clippy 为 all-target/all-feature、`-D warnings` |
| public API snapshot | 31/31，differences=0，source commit 为冻结代码候选 |
| `git diff --check` | 通过 |

### 已披露的基线与环境失败

`cargo test -p rocketmq-store` **不记录为通过**。486 个 library 测试通过后，集成 fixture
`file_store_vs_rocksdb_behavior_parity_after_restart` 继续复现 main 已知失败：fixture 配置 RocksDB store type，
但构造路径仍为 LocalFileMessageStore，重启后的伪 Rocks 视图没有逻辑队列。该测试文件相对 Main 基线无差异；
真实 RocksDB foundation/semantics 与 Broker Rocks/POP 专项全部通过。

Windows 首次 Broker 链接触发 MSVC `LNK1140` PDB 容量上限；按链接器建议使用 `/PDB:NONE` 且 `-j 1`
重新验证，Broker 与 workspace 门禁通过。并行 MCP 链接曾因多个 linker 争用同一个临时 PDB 触发 `LNK1201`，
限制为单 job 后 73/73 library 与集成测试通过。期间磁盘不足，按交付约定执行 `cargo clean`，释放 189.2 GiB；
这些环境处理没有修改仓库配置。

## 审查与回滚

`[REV]` 结论：Index/Compaction 的 incomplete/corrupt generation 不会成为 current，active reader lease 会阻止
删除，Compaction 不依赖原 CommitLog payload 且不会裸回退扫描；既有持久格式、主写 ack 和公共路径保持不变。

回滚只允许把 `CURRENT` 原子指回重新通过 CRC、条数、边界和相同 kill/restart corpus 的 previous generation。
若 previous 不存在或验证失败，保持 `Recovering`/readiness=false，保留当前代、delta 与所需 WAL，修复后从 durable
watermark 幂等 replay；不得重写消息数据，也不得用裸 CommitLog 读取作为降级路径。
