# PR-M07-01 Legacy Runtime 兼容与回滚 Ledger

## 元数据

| 字段 | 值 |
|---|---|
| 所属里程碑 | M07 Legacy Runtime 排空与 Client 依赖边收敛 |
| 状态 | PR-M07-01 已完成；M07 继续进行 |
| canonical owner | `rocketmq-runtime` |
| compatibility facade | package `rocketmq-rust` 的 schedule/task/shutdown/root re-export |
| 下一工作包 | PR-M07-02 删除 MCP 冗余 Client 边 |

## 目标完成定义

- schedule/task/shutdown/signal owner 只存在于 runtime；legacy 文件不再拥有实现。
- runtime 不依赖 RocketMQ 内部 crate，不包含 ArcMut、WeakArcMut 或 SyncUnsafeCellWrapper。
- root workspace consumer 全部使用 runtime canonical path；standalone Example 保留并验证旧 signal path。
- schedule/cancel、broadcast signal、service shutdown 与 process signal 的退出顺序有可执行测试或 source contract。
- 新边界 crate 对 legacy runtime 的禁边进入 architecture policy；无第二 RuntimeOwner 或 detached work。

## Owner 与兼容路径

| 能力 | canonical path | R0/R1 compatibility path |
|---|---|---|
| scheduler types 与 trigger | `rocketmq_runtime::schedule::{executor,scheduler,task,trigger}` | `rocketmq_rust::schedule::*` 与 legacy 根 re-export |
| ScheduledTaskManager | `rocketmq_runtime::schedule::simple_scheduler` | `rocketmq_rust::schedule::simple_scheduler` |
| ServiceManager/ServiceTask | `rocketmq_runtime::task` | `rocketmq_rust::task`；`rocketmq_rust::service_manager!` |
| broadcast Shutdown | `rocketmq_runtime::Shutdown` | `rocketmq_rust::Shutdown` |
| process signal | `rocketmq_runtime::{wait_for_signal,wait_for_signal_result}` | `rocketmq_rust::wait_for_signal` |

legacy `schedule.rs`、`task.rs`、`shutdown.rs` 只包含 module-level glob re-export；旧 schedule 子文件和
`task/service_task.rs` 已从 legacy 物理删除。Rust compatibility tests 通过 assignment 证明三类 stateful type 的新旧路径
共享类型身份，而不是复制 facade 类型。

## 退出顺序

1. process owner 等待 SIGTERM/SIGINT/Ctrl-C；fallible registration error 进入 `RuntimeError::LifecycleOperation`，
   compatibility `wait_for_signal()` 只记录失败，不 panic。
2. ScheduledTaskManager 先 cancel driver token，再等待选定或全部 driver；deadline 到达后 abort 未完成 driver，最后关闭
   所属 TaskGroup 并记录 ShutdownReport。
3. ServiceManager 先将 stopped 置位并 wakeup，随后按 service join time 等待 task；interrupt 或 deadline 路径通过
   TaskGroup abort-and-wait，最后保存 ShutdownReport 并进入 Stopped。
4. broadcast Shutdown 只把首次接收记为 stopped；重复 `recv()` 立即返回，发送端关闭或 lag 只记录 warning。

不允许通过新增 `tokio::spawn`、`spawn_blocking`、`std::thread`、ad-hoc runtime 或第二 RuntimeOwner 改写上述顺序。

## Consumer Inventory

| 能力 | root workspace canonical consumer 文件数 | owner |
|---|---:|---|
| schedule | 4 | Broker 2、Client 1、Store 1 |
| ServiceManager/ServiceTask | 6 | Broker 3、Store 3 |
| broadcast Shutdown | 2 | Client 2 |
| process signal | 3 | Broker、NameServer、Remoting 各 1 |

root workspace lifecycle legacy import 为零。standalone `rocketmq-example` 保留 8 个
`rocketmq_rust::wait_for_signal` consumer，不新增 runtime manifest 边；这组调用用于验证 R0/R1 compatibility path，
并已通过 standalone all-target Clippy。

## 依赖、错误与 toolchain 约束

- `rocketmq-runtime` 仅依赖外部 runtime/scheduling 库；target DAG 仍为零内部边。
- `rocketmq-rust` 生产依赖只剩 runtime、serde、tokio；tracing 降为自带 example 的 dev-dependency。
- canonical ServiceManager/ScheduledTaskManager 的启动错误归 `RuntimeError`；Broker 需要保留 RocketMQResult 的边界通过
  `BrokerAsyncTaskFailed { source }` 保留 typed chain，不允许 `.to_string()` 降级。
- 既有 async closure compatibility 使用 `async_fn_traits`/`unboxed_closures`；gate 从 legacy owner 原样迁入 runtime，
  本包没有新增 unstable API。移除该 gate 需要单独的 public callback 设计与 stable toolchain 差分，不得在 facade 中复制实现。

## 验证证据

- runtime：41 owner unit + 34 runtime model + 2 concurrency model，共 77 项。
- legacy：33 unit；canonical/legacy compatibility 3 项。
- consumer：Broker scheduled 1、Client scheduled 4、Store all-feature lib 484；五个 workspace consumer all-feature compile。
- contracts：M07-01 source contract 6/6；architecture guard 35/35 与 6 个 violation fixtures。
- governance：runtime enforcing audit、AGENTS routing、ArcMut guard/63 tests/24 fixtures 通过；ArcMut 保持
  1,170 identities/3,232 occurrences。
- standalone：Example fmt/Clippy；Tauri backend fmt/all-feature Clippy；Web backend fmt/all-feature Clippy/all-target build。
- final：root workspace exact fmt 与 all-target/all-feature strict Clippy 通过。
- typed error：本包新增 finding 已清零；main 既有 11 项仍为失败基线，不记为通过。

## 分层回滚

1. consumer 编译或行为失败时，可逐文件恢复 `rocketmq_rust` import；因旧路径是 runtime 类型的精确 re-export，
   不改变 owner 实现或运行语义。
2. legacy external path 失败时，只修正 shim/re-export 和 compatibility test；不得把 schedule/task/shutdown 实现复制回 legacy。
3. runtime lifecycle 失败时，按 signal、broadcast Shutdown、ServiceManager、ScheduledTaskManager 四个独立切片回滚 owner
   变更，同时保留 TaskGroup/RuntimeOwner 基础设施。
4. 回滚不得恢复 panic signal registration、字符串化 source、detached spawn/thread、第二 RuntimeOwner，或已删除的 ArcMut escape。
