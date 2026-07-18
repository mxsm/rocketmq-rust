# M11-12 Soundness 收口进度证据

## 状态与完成边界

M11-12 的最终目标是 production/public compatibility API 中不存在 `ArcMut`、`WeakArcMut`、safe
`mut_from_ref` 或 clone-safe `AsMut`/`DerefMut`，并让默认 workspace 在 stable Rust 下通过，同时把 Miri/Loom、
soak、SLO fault、dashboard/runbook、rollback 和 Human Gate 绑定到同一候选快照。

该目标尚未完成。本文件记录 M11-12a～c 子切片的真实下降，不把子切片计为第 76 个工作包，也不刷新
baseline 来掩盖剩余债务。父 Issue 为 #8292；M11-12a 子切片 Issue 为 #8293；分支为
`mxsm/architecture-refactor-owned-values`。

M11-12b 的 Controller config owner 由 Issue #8295 跟踪，分支为
`mxsm/architecture-refactor-controller-config`；它仍是同一 M11-12 工作包的子切片。

M11-12c 的 Controller manager/heartbeat lifecycle owner 由 Issue #8297 跟踪，分支为
`mxsm/architecture-refactor-controller-manager`；它仍是同一 M11-12 工作包的子切片。

## 初始盘点

在 main `86719f1bc77c2e78ff32195262bad820145f271b` 上生成当前源码快照：

| 分类 | 条目 | occurrence |
|---|---:|---:|
| production | 760 | 2,125 |
| test | 380 | 未作为终态 production Gate |
| compatibility | 14 | 必须在最终切片删除 |
| 合计 | 1,154 | - |

production 条目主要集中在 Broker 299、Client 149、Store 127、Remoting 69、Controller 53、NameServer 47。
当前默认 `python scripts/arc_mut_guard.py` 还因近期 lifecycle/deadline 重排产生 19 个 source/baseline 指纹漂移；
`--current-milestone M11` 同时正确暴露大量过期债务。因此不得把 guard 描述为已通过，也不得以 relocation 或延期
替代真实删除。

## M11-12a 实现

| 目标 | 实现与证据 |
|---|---|
| Common read helper | `QueueTypeUtils` 与 cleanup policy compatibility helper 改为 `T: AsRef<TopicConfig>`，production 源码不再导入 ArcMut |
| Common stable leaf | 删除 `rocketmq-common` 未使用的 `sync_unsafe_cell` feature 及 unsafe-cell/DerefMut owner imports；测试 fixture 使用普通 `Arc<TopicConfig>` |
| Remoting response owner | `RpcResponse.header` 改为独占 `Box<dyn CommandCustomHeader>`；删除恒返 `None` 的 shared-ref mutation facade |
| Exclusive mutation | `get_header_mut` 只从 `&mut self` 返回 typed header；新增 mutation 和 canonical conversion 定向测试 |
| Wire adapter | response command 仍从 owned header 生成 ext fields，不改变 request/response code、header 字段或 wire 编码语义 |

本切片后的实际快照为 1,123 个条目：production 733、test 376、compatibility 14；production occurrence 为
2,082。相对初始快照真实删除 27 个 production 条目和 43 个 production occurrence。

## M11-12b Controller config owner

| 目标 | 实现与证据 |
|---|---|
| 单一发布 owner | `ControllerConfigHandle` 仅在 crate 内由 `ControllerManager` 持有；公开接口只返回只读 `Arc<ControllerConfig>` 快照 |
| 原子更新 | `ArcSwap` 保存 immutable snapshot，异步 Mutex 串行 writer；每次在私有 clone 上应用全部属性并执行整体 `validate()`，成功后单次 publish |
| 失败隔离 | 解析、未知属性或整体校验失败不替换 active pointer；已有 reader 继续持有原快照 |
| coherent read | Controller/OpenRaft/metadata/metrics/storage 消费者改用 `ControllerConfigReader`，每个逻辑操作只固定一个快照；启动期派生资源不虚假宣称热重配 |
| 无用 owner 删除 | Broker/Topic/Replica/Config metadata manager 与 ProcessorManager 不再保存未读取的配置 owner |
| 并发合同 | 新增旧 reader 稳定、失败 pointer equality、并发 writer 不丢更新、并发 reader 仅观察完整 old/new 组合测试 |

M11-12b 后实际快照为 1,060 个条目：production 711、test 335、compatibility 14；production occurrence 为
2,029。相对 M11-12a 删除 22 个 production 条目和 53 个 production occurrence；相对初始快照累计删除
49 个 production 条目和 96 个 production occurrence。`rocketmq-controller` 中不再存在
`ArcMut<ControllerConfig>`，但其他 Controller owner 仍有 31 个 production 条目。

## M11-12c Controller manager/heartbeat lifecycle owner

| 目标 | 实现与证据 |
|---|---|
| 安全根 owner | standalone Controller 与 NameServer embedded Controller 均使用 `Arc<ControllerManager>`；公开 lifecycle receiver 不再要求 `ArcMut<Self>` 或 whole-manager mutable access |
| 单一 lifecycle transition | Tokio async lifecycle mutex 串行 initialize/start/shutdown；initialized 只在完整初始化成功后以 release store 发布，并发 initialize 仅一个调用执行转换 |
| heartbeat 内部同步 | `DefaultBrokerHeartbeatManager` 使用内部 Mutex/RwLock 管理 scan task、schedule 与 listener snapshot；兼容 trait 的 `&mut self` 方法只委托安全 shared 方法 |
| 无强引用环 | `BrokerHousekeepingService`、`ControllerRequestProcessor` 与 inactive listener 只保存 `Weak<ControllerManager>`；后台任务退出后不会继续持有完整服务图 |
| 消费者迁移 | Controller bootstrap、examples、bench、request contracts、OpenRaft heartbeat handle 与 NameServer embedded lifecycle 全部改用安全 `Arc` owner |
| 并发生命周期合同 | 覆盖 initialize/start 串行、processor 不保活 manager、heartbeat 双 start 只拥有一个 scan task、并发 graceful shutdown 幂等归零，以及部分启动失败后的统一组件回滚 |

M11-12c 后实际快照为 1,038 个条目：production 697、test 327、compatibility 14；production occurrence 为
1,986。相对 M11-12b 删除 14 个 production 条目和 43 个 production occurrence；相对初始快照累计删除
63 个 production 条目和 139 个 production occurrence。`rocketmq-controller` production 债务由 31 条/91 occurrence
降至 17 条/51 occurrence；`rocketmq-namesrv` 仍为 47 条，但 embedded Controller 迁移使 occurrence 从 102 降至 99。

reviewed baseline 为 1,038 条、production 697/1,987 occurrences：它只批准两条同 item import 指纹 relocation，
并刻意保留一个已不存在的历史 Controller occurrence，因此默认 guard 继续以 6 个 finding 暴露 Controller/NameServer
既有 lifecycle source drift，而不是把该漂移吸收到 baseline。

## 已执行验证

| 命令 | 结果 |
|---|---|
| `cargo test -p rocketmq-common queue_type_utils --lib` | 2/2 通过 |
| `cargo test -p rocketmq-common cleanup_policy_utils --lib` | 8/8 通过 |
| `cargo test -p rocketmq-remoting rpc_response::tests --lib` | 2/2 通过 |
| `cargo test -p rocketmq-common --lib` | 627/627 通过 |
| `cargo test -p rocketmq-remoting` | 单元、集成与文档测试全部通过（文档测试 8 通过、21 忽略） |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` | 66/66 通过；新增 resolved-only pruning 负向合同 |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12-arc-mut-after-low-risk.json` | 快照生成；1,123 条目，production 733 |
| `python scripts/arc_mut_guard.py --prune-resolved target/m11-12-arc-mut-pruned.json` | 只删除源码已不存在的 identity；baseline 1,154→1,121、occurrence 3,189→3,137，保留全部 source drift |
| `python scripts/arc_mut_guard.py` | 仍失败 19 项，全部是 M11-12a 前已存在的 Controller/NameServer source drift；本切片未新增 guard violation |
| `python scripts/arc_mut_guard.py --fixtures` | 24/24 fixture 合同通过 |
| `python scripts/architecture_dependency_guard.py --mode target` | 通过；35/35 target compatibility edges 与 3/3 test edges 对齐 |
| `python scripts/architecture_dependency_guard.py --mode baseline` | 通过 |
| `python scripts/architecture_release_guard.py` | 通过；32/32 release topology、10/10 R0 crate 对齐 |
| `cargo +stable check -p rocketmq-common`（清除本机 nightly-only `RUSTFLAGS` 后） | 未通过：Common 自身的 `sync_unsafe_cell` 已消失，但依赖 `rocketmq-runtime` 仍有 `async_fn_traits` feature；stable workspace Gate 保持开放 |
| `git diff --check` | 通过 |

M11-12b 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-controller --all-targets --all-features` | 通过；library、integration targets、examples 与 benches 全部编译 |
| `cargo test -p rocketmq-controller config::tests --lib --all-features` | 6/6 通过，其中 5 项为 snapshot owner 合同 |
| `cargo test -p rocketmq-controller --all-features` | 全部通过；library 138 通过/3 忽略，bin、9 组 integration、multi-node、OpenRaft、snapshot 与 doc tests 均通过 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` | 67/67 通过；新增 reviewed-reduction 负向合同 |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12-controller-after.json` | 1,060 条目；production 711/2,029 occurrences |
| reviewed baseline reduction（临时 ADR-013 approval） | `--apply-reviewed-reductions` 仅应用 14 条同 item 一对一 relocation，并删除真实消失 occurrence；临时 approval 不提交 |
| `python scripts/arc_mut_guard.py` | 仍只失败切片前 19 项 Controller/NameServer drift；未把既存漂移写入 baseline |
| `python scripts/arc_mut_guard.py --fixtures` | 24/24 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker message 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `python scripts/architecture_dependency_guard.py --mode target` | 通过；35 条 target compatibility edge 与 3 条 test edge 对账 |
| `python scripts/architecture_dependency_guard.py --mode baseline` | 通过 |
| `python scripts/architecture_release_guard.py` | 通过；32/32 release topology、10/10 R0 crates |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12c 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-controller --all-targets --all-features` | 通过 |
| `cargo check -p rocketmq-namesrv --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-controller concurrent_ --all-features -- --nocapture` | 5/5 通过 |
| `cargo test -p rocketmq-controller --all-features` | 全部通过；library 142 通过/3 忽略，其余 bin、integration 与 doc targets 全部通过 |
| `cargo test -p rocketmq-controller startup_failure_cleanup_stops_owned_components --all-features -- --nocapture` | 1/1 通过；完整启动后模拟失败，验证 deadline-bounded 回滚、heartbeat/task slot 清零与后续 shutdown 幂等 |
| `cargo test -p rocketmq-namesrv --all-features` | 全部通过；library 179、bin/integration 与 doc targets 全部通过 |
| targeted `ArcMut<ControllerManager>`/heartbeat owner scan | `NO_TARGETED_ARCMUT` |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12-controller-manager-after-final.json` | 实际 1,038 条；production 697/1,986、test 327/940 occurrences |
| reviewed baseline reduction（临时 ADR-013 approval） | 只批准 2 条同 item import relocation，删除已解决债务；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 仍失败 6 项既有 Controller/NameServer lifecycle drift；未写入 baseline |
| `cargo test -p rocketmq-broker three_controller_two_broker_controller_mode_bootstrap --lib --all-features -- --nocapture` | 1/1 通过；验证 Broker 跨 crate Controller cluster fixture 使用安全 `Arc` owner |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `python scripts/arc_mut_guard.py --fixtures` / guard unit tests | 24/24 fixtures、67/67 单测通过 |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |

## 剩余切片与 Gate

1. Remoting Channel/ConnectionHandlerContext 单 writer owner 和有界发送 capability。
2. Controller Raft/remoting-client owner 与 NameServer v1 tables/runtime 安全 owner；Manager/heartbeat shell 已完成。
3. Client owned-message、MQClientInstance、Producer/Admin、Push/Lite Consumer owner。
4. Broker TopicConfig/offset、BrokerRuntimeInner、schedule/POP/processor/transaction owner。
5. Store TopicConfig snapshot、MappedFileQueue/ConsumeQueue、CommitLog/Flush、StoreHandle/Rocks/Timer 与 HA actor。
6. 删除 compatibility `arc_mut.rs` 和公开 re-export，移除其余 nightly feature，将 guard 切到 production/public zero。
7. 对同一候选快照执行 stable feature matrix、Miri/Loom 可用切片、soak/SLO fault、dashboard/runbook、动态
   Kind/K3d/container、M10 固定硬件和 Human Gate。

任何切片失败都只回滚对应独立 PR，不扩大 baseline，不删除 durability/fault 证据，也不把 fixture 当作动态 PASS。
