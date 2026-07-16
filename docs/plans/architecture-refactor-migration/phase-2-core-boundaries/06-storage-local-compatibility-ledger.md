# M06-03 Local 存储兼容与所有权 Ledger

本文冻结 PR-M06-03 完成后的 Local 存储 canonical owner、`rocketmq-store` 兼容面、后续迁移边界和删除条件。
它只关闭 CommitLog append/load/recovery、MappedFile、MappedFileQueue 本阶段纯 owner 与 allocation service；不会提前关闭
M06-04～M06-08 的 flush、CQ/Index、HA、Timer/POP 或 `LocalFileMessageStore` composition。

## Canonical ownership

| Surface | Canonical owner | `rocketmq-store` compatibility / adapter | Removal milestone |
|---|---|---|---|
| `MappedFile` contract、default/native mapping、direct I/O、select result、metrics | `rocketmq-store-local::mapped_file` | `log_file::mapped_file`、`memory` 与 `default_mapped_file_impl` 保留旧路径的精确 re-export/type alias；Store 只保平台探测与业务 adapter | M06-11 冻结 facade；旧 public path 最早在下一 major 且有 consumer 迁移证据后删除 |
| MappedFileQueue storage/state/index/allocation/load-create/lifecycle/maintenance/metrics | `rocketmq-store-local::mapped_file::queue_*` | Store concrete queue 保留 ArcSwap snapshot、对象身份、日志和 I/O 副作用应用，不复制 Local plan/algorithm | M06-04/05 迁完 flush/CQ port，M06-08 收敛 composition |
| AllocateMappedFileService worker、request queue/table、timeout 与 mapped-file create | `rocketmq-store-local::base::allocate_mapped_file_service` | `base::allocate_mapped_file_service::AllocateMappedFileService` 为精确 re-export；Store 仅投影 `MessageStoreConfig` | M06-08 收敛 config；旧 re-export 按下一 major 兼容窗口处理 |
| CommitLog record/header/parser、append frame/traversal/attempt/resolution | `rocketmq-store-local::commit_log` | Store 保留 message encode、topic/put lock、MappedFile append port、legacy result、flush 与 HA 后处理 | M06-04/06 迁移后处理 port，M06-08/11 收敛 facade |
| CommitLog discovery/load/mmap hint/fast-safe route | `rocketmq-store-local::commit_log::{load,loader,load_orchestration}` | `commit_log_loader` 精确 re-export canonical types；Store 只适配目录/config、MappedFile、日志和统计 | M06-08 收敛 composition；旧 loader path 下一 major 前保留 |
| CommitLog normal/abnormal recovery state、segment driver、route、completion | `rocketmq-store-local::commit_log::{normal_recovery,abnormal_recovery,recovery,recovery_orchestration}` | `commit_log_recovery` 精确 re-export公共 recovery value；Store 只提供 record I/O/parser/dispatch 和 CQ/runtime side effects | M06-04～06 迁完外部 port 后由 M06-08/11 收敛 |
| CommitLog runtime state 与 root composition owner | `rocketmq-store-local::commit_log::{runtime_state,root}` | 旧 public `log_file::commit_log::CommitLog` 保持单字段 `CommitLogRoot<CommitLogAdapter>` facade；Store adapter 暂存 config、MappedFileQueue、dispatcher、flush/HA port | M06-08 完成 Local composition，M06-11 冻结最终 Store facade |

## Feature compatibility

`rocketmq-store-local` 保持 `default = []`，并 canonical 拥有 `fast-load`、`safe-load`、`io_uring` 和可选
`observability` feature。`rocketmq-store` 的 R0 默认仍为 `local_file_store + fast-load`，对应 feature 只向 Local 转发；
`rocksdb_store`、`tieredstore` 及旧 alias 不属于 PR-M06-03 的删除范围。fast/safe 优先级、默认加载模式和磁盘格式均未改变。

## Retained Store-only ports

- M06-04：FlushManager、GroupCommit request/worker、checkpoint 和 SyncFlush/ack adapter。
- M06-05：ConsumeQueue、Index、dispatch materialization 与 CQ timestamp。
- M06-06：HA、replication、transfer 和 controller/replica side effects。
- M06-07：Timer、POP、revive、stats 与 Local services。
- M06-08：`LocalFileMessageStore` lifecycle/query/reput/cleanup composition 和 legacy config envelope。

这些 port 留在 Store 不代表 CommitLog/MappedFile algorithm owner 回流；它们必须在对应后续 PR 独立迁移并保留各自行为、生命周期与
持久兼容证据。

## Compatibility and removal rules

- R0 的 Store public type/module path、feature alias、Serde/default 和数据目录继续可编译；本 PR 不删除 deprecated path。
- Local 不依赖 Store facade、Broker、Remoting、RocksDB 或 TieredStore；Rocks/Tiered 不得创建第二 CommitLog。
- Store facade 只能保留精确 re-export、type alias、composition、config 投影和外部副作用 adapter；不得重新实现已列为 Local owner 的算法。
- facade 删除必须等到 M06-11/M09 冻结 consumer 清单，并至少跨越下一 major 兼容窗口；没有下游 canonical-import 证据不得删除。
- PR-M06-03 可按提交/合并整体回滚，因为旧 public path、feature 入口和磁盘数据未改变；不得通过回滚恢复已被契约禁止的双 owner。

## Closeout evidence

- `rocketmq-store-local` all-feature 单元、integration 与 doctest 全绿；Store lib 537/537。
- 完整 M06 owner/adapter/mutation contract 160/160；Local/Store strict Clippy 与 workspace fmt 全绿。
- architecture dependency、ArcMut zero-growth、AGENTS routing、enforcing runtime audit 与 diff check 全绿。
- `git fetch origin main` 后分支相对 `origin/main` 为 0 behind；PR-M06-03 没有未合入的 main 基线。
