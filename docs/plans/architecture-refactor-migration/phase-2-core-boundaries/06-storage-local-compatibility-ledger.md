# M06-03～M06-08 Local 存储兼容与所有权 Ledger

本文冻结 PR-M06-08 完成后的 Local 存储 canonical owner、`rocketmq-store` 兼容面、后续迁移边界和删除条件。
它关闭 CommitLog append/load/recovery、MappedFile、MappedFileQueue、allocation service、flush/group-commit、ConsumeQueue 与 Index
以及 backend-neutral HA/replication/transfer、Timer/POP/services/stats/filter/hook、Local store composition/config owner；不会提前关闭
M06-09～12 的 RocksDB、Tiered、最终 Store facade/capability 工作，也不会把具体 socket、Remoting/controller DTO、Broker 身份或消息存储副作用搬入 Local。

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
| FlushManager root、GroupCommit request/batch/worker、queue flush/commit 与 real-time workers | `rocketmq-store-local::flush::{root,group_commit,queue,worker}` | 旧 `DefaultFlushManager` 保持单字段 `FlushManagerRoot<DefaultFlushManagerAdapter>` facade；Store adapter 只持有 config、TaskGroup、mapped-file I/O、checkpoint、health 与消息状态映射 | M06-08 收敛 Local composition，M06-11 冻结最终 Store facade；旧 public path 按下一 major 兼容窗口处理 |
| ConsumeQueue 20B/46B records、CQExt、single/batch scan/recovery/truncate/search 与 root/store/dispatch driver | `rocketmq-store-local::consume_queue::{record,single,batch,extension,root}` | Store 保留 mapped-file I/O、CQ factory/table、config/checkpoint、CommitLog timestamp lookup、日志与 `DispatchRequest`/CQType 投影；旧 CQ/CQExt public path 精确 re-export 或窄 adapter | M06-08 收敛 Local composition，M06-11 冻结最终 Store facade；持久记录与旧 public path 按下一 major 兼容窗口处理 |
| Index 40B header、4B slot、20B entry、file put/query、service lifecycle/query/build 与 dispatch root | `rocketmq-store-local::index::{codec,file,service,dispatch}` | `IndexService` 与 dispatcher 保持单字段 Local root facade；Store 仅保留目录/mmap I/O、RwLock/Arc 文件表、config/checkpoint/error flag、shared Java hasher、等待/flush runtime adapter 与日志；`QueryOffsetResult` 精确 re-export | M06-08 收敛 Local composition，M06-11 冻结最终 Store facade；磁盘格式与旧 public path 按下一 major 兼容窗口处理 |
| HA wire/flow、replication state/progress/ACK、transfer planner/segment/engine/metrics | `rocketmq-store-local::{ha,transfer}` | Store 旧 HA/transfer path 精确 re-export canonical value/engine；具体 socket、controller Remoting DTO、CommitLog/LocalFileMessageStore append、config/logging 与 ServiceContext/TaskGroup lifecycle 作为窄 adapter 保留 | M06-08 收敛 Local store composition，M06-11 冻结最终 Store facade；旧 public path 按下一 major 兼容窗口处理 |
| Timer slot/log/wheel、40B record codec、56B checkpoint state/codec、metrics、schedule/recovery/backlog/TPS | `rocketmq-store-local::timer` | 旧 slot/log/wheel path 精确 re-export；Store 只保留 MessageExt/CommitLog/CQ、config、checkpoint/metrics 文件、DataVersion/ConfigManager 与 TaskGroup scheduler adapter | M06-08 收敛 Local store composition，M06-11 冻结最终 Store facade；磁盘格式与旧 public path 按下一 major 兼容窗口处理 |
| POP ACK/BatchACK/Checkpoint 与 AckMessage contract | `rocketmq-store-local::pop` | Store 旧 `pop` module/leaf path 精确 re-export，不复制 Serde、offset/revive 或排序算法 | M06-08 收敛 Local composition，M06-11 冻结 facade；旧 public path 按下一 major 兼容窗口处理 |
| Store stats type、counter/snapshot、TPS、latency bucket 与 runtime-info state | `rocketmq-store-local::stats` | `StoreStatsService` 只组合 Local `StoreStatsState`，保留 BrokerIdentity、TaskGroup scheduler 与 start/stop adapter；旧 StatsType path 精确 re-export | M06-08 收敛 composition，M06-11 冻结 facade |
| Message filter、cold-data service 与 ordered hook registry | `rocketmq-store-local::{filter,services,hook}` | 旧 filter/cold-data path 精确 re-export；Store hook trait 继续投影 MessageExt/PutMessageResult，仅由 Local 泛型 registry 持有有序 Arc 列表 | M06-08 收敛 composition；旧 public path 和 Store hook trait 按 consumer 迁移证据处理 |
| Local store lifecycle/query/reput/cleanup composition | `rocketmq-store-local::message_store::{lifecycle,query,reput,cleanup,local_file_message_store}` | Store 保留 public `LocalFileMessageStore`、Broker/MessageExt、CommitLog/CQ/Index、文件锁、RunningFlags、checkpoint 与 TaskGroup effect adapter；状态和决策委托 `LocalStoreComposition` | M06-11 冻结最终 facade；M06-12 将 126 方法 legacy trait 收敛到 capability |
| Local normalized backend config | `rocketmq-store-local::config::backend` | Store `MessageStoreConfig` 是唯一 legacy Serde/default/alias envelope，通过 `normalized_local_backend_config()` 投影 immutable paths/recovery/query/reput/cleanup/I/O settings | M06-11 冻结 feature/factory；旧 envelope 按下一 major 兼容窗口处理 |

## Feature compatibility

`rocketmq-store-local` 保持 `default = []`，并 canonical 拥有 `fast-load`、`safe-load`、`io_uring` 和可选
`observability` feature。`rocketmq-store` 的 R0 默认仍为 `local_file_store + fast-load`，对应 feature 只向 Local 转发；
`rocksdb_store`、`tieredstore` 及旧 alias 不属于 PR-M06-03 的删除范围。fast/safe 优先级、默认加载模式和磁盘格式均未改变。

## Retained Store-only ports

- M06-08 完成后仅保留 Store 外部 adapter：legacy `MessageStoreConfig` envelope/normalized 投影、`TaskGroup`/timer/blocking executor、具体 mapped-file
  I/O、`StoreCheckpoint`、health recorder 与 `GroupCommitStatus -> PutMessageStatus` 映射；这些 adapter 不拥有 Local flush 算法。
- ConsumeQueue/Index 仅保留具体 mapped-file/file-table/factory、checkpoint、CommitLog timestamp、shared Java hasher、
  `DispatchRequest`/MessageConst/config、error flag、日志以及受审计的 wait/detached-flush side effects；不得复制 Local codec 或 driver。
- HA/replication/transfer 仅保留具体 socket/network、controller Remoting DTO、CommitLog/`LocalFileMessageStore` append、
  config/logging、`ServiceManager`/`ServiceContext`/`TaskGroup` 与 controller/replica side effects；不得复制 Local wire、
  flow-control、replication state/progress/ACK 或 transfer driver。
- Timer 仅保留 `MessageExt`/CommitLog/CQ effect、config、checkpoint/metrics 文件、Remoting `DataVersion` 与 TaskGroup；
  POP/filter/cold-data/stats 旧路径只作 re-export 或 lifecycle adapter，不得复制 Local codec/state/algorithm。
- `LocalFileMessageStore` 继续保留公共 facade、legacy trait 与具体 I/O/runtime effects；lifecycle/query/reput/cleanup 状态和决策已由
  Local composition 拥有。Store 9K legacy 文件的兼容 trait/tests 将在 M06-12 capability 收敛时拆除，不以新增 ArcMut path identity 换取机械行数拆分。

这些 port 留在 Store 不代表 CommitLog/MappedFile/CQ/Index algorithm owner 回流；它们必须在对应后续 PR 独立迁移并保留各自行为、生命周期与
持久兼容证据。

## Compatibility and removal rules

- R0 的 Store public type/module path、feature alias、Serde/default 和数据目录继续可编译；本 PR 不删除 deprecated path。
- Local 不依赖 Store facade、Broker、Remoting、RocksDB 或 TieredStore；Rocks/Tiered 不得创建第二 CommitLog。
- Store facade 只能保留精确 re-export、type alias、composition、config 投影和外部副作用 adapter；不得重新实现已列为 Local owner 的算法。
- facade 删除必须等到 M06-11/M09 冻结 consumer 清单，并至少跨越下一 major 兼容窗口；没有下游 canonical-import 证据不得删除。
- PR-M06-03/04/05/06/07/08 可分别按提交/合并整体回滚，因为旧 public path、feature 入口、wire bytes 和磁盘数据未改变；不得通过回滚恢复已被契约禁止的双 owner。

## Closeout evidence

- `rocketmq-store-local` 184 个 unit tests及全部 integration/doctest、Store LocalFileMessageStore focused 83/83、
  legacy config 35/35 和 public-path doctest 全绿；default/alias/path/ratio projection 与 lifecycle/query/reput/cleanup 均有回归。
- M06-08 composition source contract、M06-07 impacted contract、architecture dependency、StoreLocal owner/forbidden-edge
  contract 全绿；Local 保持 Store/Common/Remoting/Broker/ArcMut/detached-task 禁边。
- Local/Store strict Clippy、workspace `--no-deps --all-targets --all-features -D warnings` Clippy、workspace fmt、
  AGENTS routing、enforcing runtime audit、architecture guard 与 diff check 全绿。
- ArcMut 14 条同 item fingerprint relocation 按 ADR-013 精确批准，monotonic promotion/compare、final guard 与 24 项
  fixture 全绿；账本保持 1,171 identities/3,233 occurrences，零新增债务。
- `git fetch origin main` 后候选分支必须保持 0 behind；PR-M06-08 不得包含未合入的 main 基线。
