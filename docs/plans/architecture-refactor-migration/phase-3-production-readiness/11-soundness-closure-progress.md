# M11-12 Soundness 收口进度证据

## 状态与完成边界

M11-12 的最终目标是 production/public compatibility API 中不存在 `ArcMut`、`WeakArcMut`、safe
`mut_from_ref` 或 clone-safe `AsMut`/`DerefMut`，并让默认 workspace 在 stable Rust 下通过，同时把 Miri/Loom、
soak、SLO fault、dashboard/runbook、rollback 和 Human Gate 绑定到同一候选快照。

该目标尚未完成。本文件记录 M11-12a～m 子切片的真实下降，不把子切片计为第 76 个工作包，也不刷新
baseline 来掩盖剩余债务。父 Issue 为 #8292；M11-12a 子切片 Issue 为 #8293；分支为
`mxsm/architecture-refactor-owned-values`。

M11-12b 的 Controller config owner 由 Issue #8295 跟踪，分支为
`mxsm/architecture-refactor-controller-config`；它仍是同一 M11-12 工作包的子切片。

M11-12c 的 Controller manager/heartbeat lifecycle owner 由 Issue #8297 跟踪，分支为
`mxsm/architecture-refactor-controller-manager`；它仍是同一 M11-12 工作包的子切片。

M11-12d 的 Controller Raft owner 由 Issue #8299 跟踪，分支为
`mxsm/architecture-refactor-controller-raft`；它仍是同一 M11-12 工作包的子切片。

M11-12e 的 Controller request processor owner 由 Issue #8301 跟踪，分支为
`mxsm/architecture-refactor-controller-processor`；它仍是同一 M11-12 工作包的子切片。

M11-12f 的 NameServer runtime/processor owner 由 Issue #8303 跟踪，分支为
`mxsm/architecture-refactor-namesrv-runtime`；它仍是同一 M11-12 工作包的子切片。

M11-12g 的 Remoting Channel/ConnectionHandlerContext owner 由 Issue #8307 跟踪，分支为
`mxsm/architecture-refactor-remoting-channel-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12h 的 Remoting client/handler owner 由 Issue #8309 跟踪，分支为
`mxsm/architecture-refactor-remoting-client-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12i 的 NameServer V1 table owner 由 Issue #8311 跟踪，分支为
`mxsm/architecture-refactor-namesrv-v1-tables`；它仍是同一 M11-12 工作包的子切片。

M11-12j 的 Remoting protocol compatibility 由 Issue #8313 跟踪，分支为
`mxsm/architecture-refactor-remoting-protocol-compat`；它仍是同一 M11-12 工作包的子切片。

M11-12k 的 Client ProduceAccumulator owner 由 Issue #8315 跟踪，分支为
`mxsm/architecture-refactor-client-produce-accumulator`；它仍是同一 M11-12 工作包的子切片。

M11-12l 的 Client latency fault detector owner 由 Issue #8317 跟踪，分支为
`mxsm/architecture-refactor-client-latency-fault`；它仍是同一 M11-12 工作包的子切片。

M11-12m 的 Client message ownership 由 Issue #8319 跟踪，分支为
`mxsm/architecture-refactor-client-pull-result`；它仍是同一 M11-12 工作包的子切片。

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

## M11-12d Controller Raft owner

| 目标 | 实现与证据 |
|---|---|
| OpenRaft 内部生命周期 | `OpenRaftController` 以 Tokio async mutex 串行 startup/shutdown transition，以短临界区 Mutex 保存 node 与 gRPC shutdown handle；同步锁不跨 `.await` |
| 失败与幂等语义 | listener 绑定成功后才创建并发布 Raft node；任务注册失败会关闭 node/task group；重复或并发 startup/shutdown 串行并返回一致结果 |
| 安全共享 owner | `RaftController.inner`、`ControllerManager.raft_controller` 与所有 Broker/Topic Processor 均使用 `Arc`；兼容 `Controller` trait 的 `&mut self` lifecycle 仅委托共享方法 |
| capability 收窄 | Manager 不再通过 `mut_from_ref` 启停 Raft，Processor 不再传播可从共享引用取得可变引用的 capability；Raft/OpenRaft 定向扫描为零 |
| 行为合同 | 并发双启动只绑定一个 gRPC listener，并发双关闭清空 node、释放 listener；既有 Controller cluster/processor 行为保持 |

M11-12d 后实际快照为 1,031 个条目：production 690、test 327、compatibility 14；production occurrence 为
1,961。相对 M11-12c 删除 7 个 production 条目和 25 个 production occurrence；相对初始快照累计删除
70 个 production 条目和 164 个 production occurrence。`rocketmq-controller` production 债务由 17 条/51 occurrence
降至 10 条/26 occurrence，全部剩余项属于 remoting client 或 `ConnectionHandlerContext` 边界；`rocketmq-namesrv`
保持 47 条/99 occurrence。

reviewed baseline 为 1,031 条、production 690/1,964 occurrences。本切片没有批准 relocation，只删除真实消失的
identity/occurrence；默认 guard 仍精确失败 6 项既有 Controller/NameServer lifecycle source drift，未将其吸收为基线。

## M11-12e Controller request processor owner

| 目标 | 实现与证据 |
|---|---|
| 共享 processor owner | `ControllerRequestProcessorWrapper` payload 由 `ArcMut` 改为 `Arc<ControllerRequestProcessor>`；wrapper clone 只共享不可变 processor |
| receiver 收窄 | request router 与 12 个具体业务 handler 全部由 `&mut self` 收窄为 `&self`；处理器字段本身保持 immutable/内部共享语义 |
| compatibility adapter | remoting `RequestProcessor` trait 的 `&mut self` receiver 仅构造共享 dispatch future；timeout、metrics、错误映射和 request routing 仍走同一完成路径 |
| owner-cycle 合同 | wrapper clone 增加的只有 processor `Arc` strong count；processor 继续以 `Weak<ControllerManager>` 断开服务图强引用环 |

M11-12e 后实际快照为 1,029 个条目：production 688、test 327、compatibility 14；production occurrence 为
1,959。相对 M11-12d 删除 2 个 production 条目和 2 个 production occurrence；相对初始快照累计删除
72 个 production 条目和 166 个 production occurrence。`rocketmq-controller` production 债务由 10 条/26 occurrence
降至 8 条/24 occurrence，剩余项仅属于 remoting client 与 `ConnectionHandlerContext` 边界。

reviewed baseline 为 1,029 条、production 688/1,962 occurrences。临时 ADR-013 approval 只批准 13 个同 handler
receiver 收窄和 1 个相邻 import token context 的一对一 relocation，approval 不提交；默认 guard 仍精确失败切片前
6 项既有 Controller/NameServer lifecycle source drift。

## M11-12f NameServer runtime/processor owner

| 目标 | 实现与证据 |
|---|---|
| 安全 runtime 根 | `NameServerRuntimeInner` 由 `ArcMut` 改为安全 `Arc`；`Arc::new_cyclic` 只把 `Weak` handle 注入 runtime-owned child，根仍单向拥有完整服务图 |
| 原子配置快照 | Namesrv/Tokio client/server/controller config 组合为单一 `ArcSwap` immutable snapshot；同步 writer 串行 clone、解析、应用并一次发布，失败保持 pointer 与全部 active 值不变 |
| child owner 收口 | KV、V2 route、housekeeping、batch-unregistration、client/cluster/default processor 均退出 `ArcMut<NameServerRuntimeInner>`；batch receiver/task slot 和 KV persistence 使用显式内部同步 |
| processor capability | NameServer processor wrapper payload 改为 `Arc`，业务 handler 使用共享 receiver；remoting trait 的 mutable receiver 不再传播 runtime 可变 capability |
| legacy V1 外层串行 | V1 wrapper 以短临界区 Mutex 串行 legacy mutable API；shutdown 先 clone service handle 再 await，不跨 `.await` 持同步锁；V1 tables 自身 `ArcMut` 明确保留给后续切片 |
| owner/config 合同 | 新增失败更新 pointer equality、并发读者只观察完整配置组合，以及同时持有 V1/V2 route、KV、housekeeping、processor clone 时 runtime 根仍可释放的回归测试 |

M11-12f 后实际快照为 1,008 个条目：production 669、test 325、compatibility 14；production occurrence 为
1,918。相对 M11-12e 删除 19 个 production 条目和 41 个 production occurrence；相对初始快照累计删除
91 个 production 条目和 207 个 production occurrence。`rocketmq-namesrv` production 债务由 47 条/99 occurrence
降至 28 条/58 occurrence；剩余项精确为 V1 tables 16/44、remoting client 4/5 和
`ConnectionHandlerContext` boundary 8/9。Issue #8305 只校正该子类别分配，不改变总量或 reviewed baseline。

reviewed baseline 为 1,008 条、production 669/1,921 occurrences。临时 ADR-013 approval 只批准 9 条同 item
一对一 relocation，approval 不提交；baseline 1,029→1,008、occurrence 2,942→2,899。默认 guard 仍精确失败切片前
6 项既有 Controller/NameServer lifecycle source drift，未将其吸收为基线。

## M11-12g Remoting Channel/ConnectionHandlerContext owner

| 目标 | 实现与证据 |
|---|---|
| lifecycle capability | `ConnectionStateHandle` 只允许读取、订阅和关闭状态，不暴露 encoder、buffer、socket half 或其他可变 I/O 状态 |
| 唯一 writer | `ChannelInner` 以 Tokio async Mutex 串行底层 `Connection` 写入；直接响应、预编码 bytes 与有界 outbound queue 共用同一 writer lock，不跨同步锁 `.await` |
| 安全共享 owner | `Channel.inner`、handler context 与 legacy response table 分别改为 `Arc<ChannelInner>`、`Arc<ConnectionHandlerContextWrapper>` 与 `Arc<parking_lot::Mutex<_>>`；删除 `connection_mut`、`channel_inner_mut`、context `channel_mut`/`connection_mut` 和 clone-safe `AsMut` |
| consumer migration | Remoting server/client/local、Broker response/POP/Pull、Controller、NameServer 与 Proxy 全部改用 Channel send capability 或只读 lifecycle handle；测试夹具不再重建旧 `ArcMut` owner |
| 生命周期收口 | Channel close 原子移除 outbound sender、关闭状态、等待 owned `TaskGroup`、按物理连接 owner 完成 pending request；Drop 取消未结束任务并完成遗留请求 |
| 并发合同 | 两个克隆 context 并发写入时远端收到两个完整可解码帧；context clone 共享同一 owner，并立即观察另一 clone 发布的关闭状态 |

M11-12g 后实际快照为 813 个条目：production 514、test 285、compatibility 14；production occurrence 为
1,612。相对 M11-12f 删除 155 个 production 条目和 306 个 production occurrence；相对初始快照累计删除
246 个 production 条目和 513 个 production occurrence。Channel/ConnectionHandlerContext 定向扫描为零；由旧
context alias 传播的 Broker、Client、Controller、NameServer、Auth 与 Proxy 债务同步退出。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 192 | 574 |
| `rocketmq-client` | 147 | 604 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-remoting` | 21 | 41 |
| `rocketmq-namesrv` | 20 | 49 |
| `rocketmq-controller` | 4 | 6 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 为 813 条、production 514/1,615 occurrences。临时 ADR-013 approval 只批准 Pull wakeup
函数内 1 条既有 `ArcMut` occurrence 的同 item 一对一 relocation，approval 不提交；baseline 1,008→813、
occurrence 2,899→2,441。默认 guard 仍精确失败切片前 6 项 Controller/NameServer lifecycle source drift；实际
bootstrap 快照为 514/1,612，没有把这 3 个 occurrence 差值描述为剩余源码债务。

## M11-12h Remoting client/handler owner

| 目标 | 实现与证据 |
|---|---|
| handler 并发边界 | `RemotingGeneralHandler` 改为安全 `Arc` owner；每个请求克隆只携带安全共享状态的 processor adapter，再调用兼容 `&mut self` trait，不用全局 async Mutex 串行所有连接，也不跨 `.await` 持锁 |
| hook capability | RPC hooks 由短临界区 `parking_lot::RwLock` 管理；请求调用前取得 `Arc` hook 快照，注册/清理只需要共享引用 |
| client lifecycle | `RemotingService` 与所有直接 owner 改用标准 `Arc`/`Weak`；shutdown、shutdown report、health cleanup 与 hook capability 不再通过 `mut_from_ref` 取得共享可变引用 |
| NameServer 选择状态 | configured list、chosen address 与 available set 由显式 `RwLock` 管理；网络 probe 前取得 owned snapshot，不跨 `.await` 持有同步 guard |
| consumer migration | Remoting server/client/RPC、Broker outer API、Client API、Controller 与 NameServer runtime 全部迁移到安全 client owner；NameServer list API 返回 owned `Vec` 快照，调用方不能持有可失效借用 |
| 并发合同 | 两个任务反复发布不同 NameServer 地址集合时，reader 只观察空或完整 owned snapshot；重复 start/shutdown 的 TaskGroup 测试继续证明 owned task 被回收 |

M11-12h 后实际快照为 771 个条目：production 488、test 269、compatibility 14；production occurrence 为
1,559。相对 M11-12g 删除 26 个 production 条目和 53 个 production occurrence；相对初始快照累计删除
272 个 production 条目和 566 个 production occurrence。Remoting client/handler 定向债务清零，Controller
production 债务清零；Remoting 只剩 3 个 protocol compatibility 文件 6/9，NameServer 只剩 V1 tables 16/44。
剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 569 |
| `rocketmq-client` | 146 | 599 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-namesrv` | 16 | 44 |
| `rocketmq-remoting` | 6 | 9 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 只删除源码已消失的 identity/occurrence，不需要 relocation approval；baseline 813→771、
occurrence 2,441→2,357，分类精确为 production 488/1,559、test 269/758、compatibility 14/40。此前 6 项
Controller/NameServer source drift 随对应 owner 删除而消失，默认 `python scripts/arc_mut_guard.py` 通过。

## M11-12i NameServer V1 table owner

| 目标 | 实现与证据 |
|---|---|
| 单一 V1 owner | 六张 route table 从 `ArcMut<HashMap<...>>` 改为 `RouteInfoManager` 独占普通 `HashMap`；table 不能脱离 manager 单独克隆或传播写 capability |
| 排他 mutation | registration、topic/permission update 等变更入口恢复 `&mut self`；既有 `Mutex<RouteInfoManager>` wrapper 成为唯一共享写边界，删除内部冗余 `RwLock<()>` |
| 复合一致性 | Broker/cluster/live/filter/topic/mapping 表仍在同一次 wrapper guard 内完成注册、注销与清理；读路径取得同一 manager guard，不观察半完成复合更新 |
| async 边界 | V1 manager guard 内没有 `.await`；shutdown 仍先克隆 unregister-service handle 再等待，通知任务只捕获 owned request/address |
| 并发合同 | 真实 remoting 并发注册两个 V1 Broker/双 topic，随后读取两份完整 route，再并发注销并确认 topic 清理；保留 V1 首次注册必须包含多个 topic 的既有兼容语义 |

M11-12i 后实际快照为 753 个条目：production 472、test 267、compatibility 14；production occurrence 为
1,515。相对 M11-12h 删除 16 个 production 条目和 44 个 production occurrence；相对初始快照累计删除
288 个 production 条目和 610 个 production occurrence。`rocketmq-namesrv` production 债务从 16/44 降至零。
剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 569 |
| `rocketmq-client` | 146 | 599 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-remoting` | 6 | 9 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 删除 `route_info_manager.rs` 中真实消失的 16 个 production identity/44 个 occurrence，以及
过时结构尺寸测试中的 2 个 test identity/2 个 occurrence，不需要 relocation approval；baseline 771→753、
occurrence 2,357→2,311，分类精确为 production 472/1,515、test 267/756、compatibility 14/40。默认
`python scripts/arc_mut_guard.py` 通过。

## M11-12j Remoting protocol compatibility

| 目标 | 实现与证据 |
|---|---|
| canonical wire owner | Remoting topic-config wrapper 不再维护 `DashMap<CheetahString, ArcMut<_>>` 镜像，直接 re-export `rocketmq-protocol` 的 owned `HashMap` DTO；Serde 字段名和 register-broker 编码仍由同一 canonical 类型负责 |
| header capability | 删除固定接受 `ArcMut<Box<dyn CommandCustomHeader>>` 的 deprecated facade；RPC response 使用 owned boxed-header setter，header materialize/decode 保持完整 |
| mapping mutation | 删除接受 `ArcMut<TopicQueueMappingDetail>` 的 deprecated helper；canonical mapping detail 只通过 `&mut` 独占引用变更 |
| consumer migration | Broker 把内部 TopicConfig/mapping manager 的共享值克隆成 owned protocol snapshot；NameServer V1/V2 使用普通 `HashMap` 键值迭代，不再依赖 DashMap guard API |
| compatibility contract | M04 测试改为 canonical DTO 类型同一性、Serde round-trip 与独占 mapping mutation；删除只固化危险签名的 legacy header test |

M11-12j 后实际快照为 747 个条目：production 466、test 267、compatibility 14；production occurrence 为
1,505。相对 M11-12i 删除 6 个 production 条目和 10 个 production occurrence；相对初始快照累计删除
294 个 production 条目和 620 个 production occurrence。`rocketmq-remoting` production 债务从 6/9 降至零，
Broker 的增量注册 wire snapshot 同步减少 1 个 occurrence。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 146 | 599 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 删除三个 Remoting protocol compatibility 文件中真实消失的 6 个 identity/9 个 occurrence，
以及 Broker wire-wrapper 的 1 个 occurrence，不需要 relocation approval；baseline 753→747、occurrence
2,311→2,301，分类精确为 production 466/1,505、test 267/756、compatibility 14/40。默认
`python scripts/arc_mut_guard.py` 通过。

## M11-12k Client ProduceAccumulator owner

| 目标 | 实现与证据 |
|---|---|
| 安全共享 owner | `MQClientManager` accumulator table 与 `DefaultMQProducer` 统一使用 `Arc<ProduceAccumulator>`；producer clone 不再传播可从共享引用取得可变引用的 accumulator capability |
| 原子运行时配置 | batch delay、单批大小与总容量限制使用 acquire/release 原子 load/store；既有校验边界、默认值和运行时 setter 可见性保持不变 |
| 显式 guard lifecycle | sync/async guard 的 task handle 与 schedule sender 收入各自 lifecycle mutex；start 串行且幂等，schedule 只克隆 sender 快照 |
| async lock 边界 | shutdown 在短同步临界区内清空 sender、取出 owned task handle，释放锁后才等待 tracked task；同步锁不跨 `.await` |
| 并行批次能力 | accumulator send API 收窄为 `&self`，仍由每个 aggregation key 的异步批次锁保护，不引入 accumulator 级全局异步锁或跨 topic 串行化 |
| 行为合同 | 新增共享 `Arc` 配置跨线程可见与重复 start 单 task 合同；既有容量并发、deadline、pending batch release 与 lifecycle probe 全部保持通过 |

M11-12k 后实际快照为 744 个条目：production 463、test 267、compatibility 14；production occurrence 为
1,495。相对 M11-12j 删除 3 个 production 条目和 10 个 production occurrence；相对初始快照累计删除
297 个 production 条目和 630 个 production occurrence。`rocketmq-client` production 债务从 146/599 降至
143/589；ProduceAccumulator 共享 owner 已退出 `ArcMut`。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 143 | 589 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 只删除源码真实消失的 3 个 identity/10 个 occurrence，不需要 relocation approval；baseline
747→744、occurrences 2,301→2,291，分类精确为 production 463/1,495、test 267/756、compatibility 14/40。

## M11-12l Client latency fault detector owner

| 目标 | 实现与证据 |
|---|---|
| 安全共享 owner | `LatencyFaultTolerance::start_detector`、实现、`MQFaultStrategy`、available/reachable filters、probe 与 tests 全部使用标准 `Arc`，latency production 不再传播共享可变 facade |
| 原子 detector 配置 | detect timeout、interval 与 enabled flag 使用 acquire/release 原子 load/store；Java-compatible 默认值和 constructor/runtime setter 语义保持 |
| 依赖快照 | resolver 与 service detector 以 `RwLock<Option<Arc<_>>>` 发布；每轮检测只在短临界区克隆快照，网络 resolve/detect await 不持配置锁 |
| 单一 task lifecycle | start 的既有 task 检查与新 scheduled task 发布合并到同一 lifecycle mutex；stopping 状态排除 shutdown 期间 restart，取消安全 reset 恢复后续可启动状态 |
| 行为合同 | 8 路并发 start 只发布一个 owned scheduled task；同步 abort、异步 deadline shutdown、probe schedule metrics、broker availability/reachability 与 config copy 合同保持通过 |

M11-12l 后实际快照为 732 个条目：production 454、test 264、compatibility 14；production occurrence 为
1,481。相对 M11-12k 删除 9 个 production 条目/14 个 production occurrence，以及 3 个 test 条目/4 个
test occurrence；相对初始快照累计删除 306 个 production 条目和 644 个 production occurrence。
`rocketmq-client` production 债务从 143/589 降至 134/575；latency production 债务清零。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 134 | 575 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 只删除三个 latency 文件中真实消失的 12 个 identity/18 个 occurrence，不需要 relocation
approval；baseline 744→732、occurrences 2,291→2,273，分类精确为 production 454/1,481、test 264/752、
compatibility 14/40。

## M11-12m Client message ownership

| 目标 | 实现与证据 |
|---|---|
| owned pull result | `PullResult` 直接持有 `Vec<MessageExt>`；与 `PullOutcome<MessageExt>` 的 non-empty/empty/absent 转换均无失败分支，兼容错误名收敛为 `Infallible` |
| 安全共享消息 | ProcessQueue/store、Push/Pop consume request、hook、trace 与 Lite zero-copy 全部使用标准 `Arc<MessageExt>`；Client production 不再出现 `ArcMut<MessageExt>` |
| 局部写隔离 | retry topic、namespace、reconsume count 与 consume timestamp mutation 使用 `Arc::make_mut`；测试证明任务副本修改不会改变队列保留值 |
| 生命周期元数据 | ProcessQueue 在既有 `RwLock` 状态内按 queue offset 跟踪 consume start timestamp；clean-expired 保留旧属性 fallback，remove/clear/replacement 同步清理元数据 |
| 行为合同 | pull decode/tag filter/offset delta、ProcessQueue count/size/span/take/rollback/commit、Push retry 与 Lite poll 合同保持通过 |

M11-12m 后实际快照为 709 个条目：production 440、test 255、compatibility 14；occurrence 精确为
production 1,397、test 720、compatibility 40。相对 M11-12l 删除 14 个 production 条目/84 个 production
occurrence，以及 9 个 test 条目/32 个 test occurrence；相对初始快照累计删除 320 个 production 条目和
728 个 production occurrence。`rocketmq-client` production 债务从 134/575 降至 120/491；production
`ArcMut<MessageExt>` 消息流清零。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 120 | 491 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 从 732→709、occurrences 从 2,273→2,157。23 个删除 identity 均来自源码真实删除；另有
9 个同 item、同 service-owner 参数的一对一 fingerprint 更新，因为相邻消息参数从 `ArcMut<MessageExt>` 改为
`Arc<MessageExt>`，未增加或移动任何共享可变 occurrence。

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

M11-12d 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-controller --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-controller shared_lifecycle_serializes_startup_and_shutdown --all-features -- --nocapture` | 1/1 通过；验证并发双启动/双关闭串行、node 发布/清空及 listener 绑定/释放 |
| `cargo test -p rocketmq-controller --all-features` | 全部通过；library 142 通过/3 忽略，bin、integration、example 与 doc targets 全部通过 |
| `cargo test -p rocketmq-broker three_controller_two_broker_controller_mode_bootstrap --lib --all-features -- --nocapture` | 1/1 通过；验证跨 crate 的 3 Controller/2 Broker 引导路径 |
| targeted `ArcMut<RaftController>`/`ArcMut<OpenRaftController>`/Manager Raft `mut_from_ref` scan | `NO_TARGETED_ARCMUT` |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12-controller-raft-after.json` | 实际 1,031 条；production 690/1,961、test 327/940、compatibility 14/40 occurrences |
| reviewed baseline reduction | 无 relocation approval；只删除真实消失债务，baseline 1,038→1,031、occurrences 2,967→2,944 |
| `python scripts/arc_mut_guard.py` | 仍精确失败切片前 6 项既有 Controller/NameServer lifecycle drift；未写入 baseline |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker message 不受 `-D warnings` 管辖 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `python scripts/arc_mut_guard.py --fixtures` / guard unit tests | 24/24 fixtures、67/67 单测通过 |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |

M11-12e 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-controller --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-controller concurrent_initialize_is_serialized_and_manager_handles_do_not_form_a_cycle --all-features -- --nocapture` | 1/1 通过；wrapper clone 仅共享 processor `Arc`，processor 不保活 manager |
| `cargo test -p rocketmq-controller --all-features` | 全部通过；library 142 通过/3 忽略，bin、integration、example 与 doc targets 全部通过 |
| targeted `ArcMut<ControllerRequestProcessor>` scan | `NO_TARGETED_ARCMUT` |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12-controller-processor-after-final.json` | 实际 1,029 条；production 688/1,959、test 327/940、compatibility 14/40 occurrences |
| reviewed baseline reduction（临时 ADR-013 approval） | 只批准 14 条同 item 一对一 relocation并删除真实消失债务；baseline 1,031→1,029、occurrences 2,944→2,942；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 仍精确失败切片前 6 项既有 Controller/NameServer lifecycle drift；未写入 baseline |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker message 不受 `-D warnings` 管辖 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `python scripts/arc_mut_guard.py --fixtures` / guard unit tests | 24/24 fixtures、67/67 单测通过 |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |

M11-12f 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-namesrv --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-namesrv runtime_config_ --all-features -- --nocapture` | 2/2 通过；失败更新不发布、并发读者不观察 torn snapshot |
| `cargo test -p rocketmq-namesrv runtime_owned_service_clones_do_not_keep_root_alive --all-features -- --nocapture` | 1/1 通过；V1/V2 route、KV、housekeeping、processor clone 均不保活 runtime 根 |
| `cargo test -p rocketmq-namesrv --all-features` | 全部通过；library 182/182，bin 1/1，integration 7/7、6/6、2/2、4/4，doc 7 通过/1 忽略 |
| targeted NameServer runtime/processor/batch/V2 `ArcMut` scan | `NO_TARGETED_ARCMUT` |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12f-namesrv-runtime-after.json` | 实际 1,008 条；production 669/1,918、test 325/938、compatibility 14/40 occurrences |
| reviewed baseline reduction（临时 ADR-013 approval） | 仅批准 9 条同 item 一对一 relocation；baseline 1,029→1,008、occurrences 2,942→2,899；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 仍精确失败切片前 6 项 Controller/NameServer lifecycle drift；未写入 baseline |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `python scripts/arc_mut_guard.py --fixtures` / guard unit tests | 24/24 fixtures、67/67 单测通过 |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12g 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo test -p rocketmq-remoting cloned_contexts_share_one_serialized_channel_writer --all-features` | 1/1 通过；两个 context clone 并发发送完整帧并共享关闭状态 |
| `cargo test -p rocketmq-remoting command_snapshot_task_groups_prune_after_each_response --all-features -- --nocapture` | 1/1 通过；128 个短命 command snapshot 不关闭共享物理连接且全部 task group 被裁剪 |
| `cargo test -p rocketmq-transport --all-features` | unit、integration 与 contract targets 全部通过；doc 9 项按既有设置忽略 |
| `cargo test -p rocketmq-remoting --all-features` | 全部通过；library 119/119，integration 9/9、2/2、7/8（1 ignored）、5/5、10/10、4/4、1/1、1/1、11/11、8/8、7/7、4/4，doc 8 通过/21 忽略 |
| `cargo test -p rocketmq-controller --all-features` | 全部通过；library 142 通过/3 忽略，bin、integration、cluster 与 doc targets 全部通过 |
| `cargo test -p rocketmq-namesrv --all-features` | 全部通过；library 182/182，bin、integration 与 doc targets 全部通过（doc 1 忽略） |
| `cargo test -p rocketmq-proxy --all-features` | 全部通过；library 84/84，gRPC/remoting ingress 与 compatibility targets 全部通过 |
| `cargo test -p rocketmq-broker pop_message_processor::tests --lib --all-features` | 19/19 通过 |
| `cargo test -p rocketmq-broker --lib --all-features` | 未描述为通过：549 通过、24 失败、1 忽略；失败集中在既有 Lite 配置/全局 lifecycle tests。代表项单独运行仍在进入本切片 Channel 使用前由 `validate_consumer_group` 返回，当前 diff 在对应测试仅迁移 Channel/context fixture owner |
| `cargo check -p rocketmq-broker -p rocketmq-namesrv -p rocketmq-controller -p rocketmq-proxy --all-targets --all-features` | 通过 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone 消费者通过 |
| targeted Channel/Context mutable escape scan | `NO_TARGETED_ARCMUT`；`connection_mut`、`channel_inner_mut`、context `channel_mut`/`connection_mut` 与 `ArcMut<ConnectionHandlerContextWrapper>` 均为零 |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12g-remoting-channel-after.json` | 实际 813 条；production 514/1,612、test 285/786、compatibility 14/40 occurrences |
| reviewed baseline reduction（临时 ADR-013 approval） | 仅批准 1 条同 item 一对一 relocation；baseline 1,008→813、occurrences 2,899→2,441；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 仍精确失败切片前 6 项 Controller/NameServer lifecycle drift；未吸收到 baseline |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| `.\scripts\check-error-hygiene.ps1` | 通过；全部 typed-error/redaction/boundary mapping 合同通过 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `python scripts/arc_mut_guard.py --fixtures` / guard unit tests | 24/24 fixtures、67/67 单测通过 |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12h 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-remoting --all-targets --all-features` | 通过 |
| `cargo check -p rocketmq-controller -p rocketmq-namesrv -p rocketmq-client-rust -p rocketmq-broker --all-targets --all-features` | 通过；所有直接消费者完成全 target/全 feature 编译 |
| `cargo test -p rocketmq-remoting --all-features` | 全部通过；library 120/120，integration 与 doc targets 全部通过（按既有设置忽略 1 项 integration、21 项 doc） |
| `cargo test -p rocketmq-controller --all-features` | 全部通过；library 142 通过/3 忽略，bin、integration、cluster 与 doc targets 全部通过 |
| `cargo test -p rocketmq-namesrv --all-features --quiet` | 全部通过；library 182/182，bin/integration 全部通过，doc 7 通过/1 忽略 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 954/954，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| `cargo test -p rocketmq-broker broker_outer_api --lib --all-features --quiet` | 6/6 通过 |
| `cargo test -p rocketmq-broker broker_runtime_service_context_parents_probe_task_groups --lib --all-features --quiet` | 1/1 通过 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone 消费者通过 |
| targeted remoting-client/handler/server/RPC mutable-owner scan | `NO_TARGETED_ARCMUT`；workspace 已无 `ArcMut<RocketmqDefaultClient>` |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12h-remoting-client-after.json` | 实际 771 条；production 488/1,559、test 269/758、compatibility 14/40 occurrences |
| reviewed baseline reduction | 无 relocation approval；仅删除真实消失债务，baseline 813→771、occurrences 2,441→2,357 |
| `python scripts/arc_mut_guard.py` | 通过；此前 6 项 Controller/NameServer source drift 随 owner 清零一并消失 |
| `python scripts/arc_mut_guard.py --fixtures` / guard unit tests | 24/24 fixtures、67/67 单测通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 初次发现并修复 2 处冗余 `Vec::from` 后通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| `.\scripts\check-error-hygiene.ps1` | 通过；14 类 typed-error/redaction/boundary mapping 合同全部通过 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过 |
| `git diff --check` | 通过 |

M11-12i 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-namesrv --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-namesrv namesrv_v1_serializes_concurrent_route_table_mutations_over_remoting --all-features -- --nocapture` | 1/1 通过；首次运行按单 topic 构造触发 V1 既有首次注册兼容拒绝，改为真实双 topic 注册后通过，production 语义未修改 |
| `cargo test -p rocketmq-namesrv --test struct_size_test --all-features --quiet` | 2/2 通过；删除过时的 200B V1/V2 差值假设，改为 wrapper 独立于 manager table layout 的稳定契约 |
| `cargo test -p rocketmq-namesrv --all-features --quiet` | 全部通过；library 182/182，bin 1/1，integration 7/7、7/7、2/2、4/4，doc 7 通过/1 忽略 |
| targeted NameServer `ArcMut`/`WeakArcMut`/`mut_from_ref` scan | `NO_TARGETED_ARCMUT`；NameServer production 债务为零 |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12i-namesrv-v1-final-after.json` | 实际 753 条；production 472/1,515、test 267/756、compatibility 14/40 occurrences |
| reviewed baseline reduction | 无 relocation approval；删除 V1 tables 16 个 production identity/44 occurrences 与旧尺寸测试 2 个 test identity/2 occurrences，baseline 771→753、occurrences 2,357→2,311 |
| `python scripts/arc_mut_guard.py` | 通过 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` | 67/67 通过 |
| `python scripts/arc_mut_guard.py --fixtures` | 24/24 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12j 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-remoting -p rocketmq-broker -p rocketmq-namesrv --all-targets --all-features` | 通过；canonical DTO 的全部直接消费者完成编译 |
| `cargo test -p rocketmq-remoting --test m04_compatibility_facades --all-features --quiet` | 9/9 通过；owned DTO 同一性、Serde round-trip 与独占 mapping mutation 通过 |
| `cargo test -p rocketmq-remoting rpc_response_command_preserves_owned_boxed_header --lib --all-features --quiet` | 1/1 通过；首次测试错误假设既有带-header路径保留非零 response code，收窄为本切片 owned header 合同后通过，production response-code 语义未修改 |
| `cargo test -p rocketmq-protocol --all-features --quiet` | 全部通过；library 1,369/1,369，integration 4/4 |
| `cargo test -p rocketmq-remoting --all-features --quiet` | 全部通过；library 121/121，全部 integration 与 doc targets 通过（按既有设置忽略 1 项 integration、21 项 doc） |
| `cargo test -p rocketmq-namesrv --all-features --quiet` | 全部通过；library 182/182，其余 bin/integration/doc targets 全部通过（doc 1 忽略） |
| `cargo test -p rocketmq-namesrv --test route_info_manager_integration --all-features --quiet` | 最终 canonical identity 清理后 7/7 通过 |
| `cargo test -p rocketmq-broker register_broker_request_parts_preserves_compression_header_and_body --lib --all-features --quiet` | 1/1 通过；压缩/非压缩 register body 均可解码 |
| `cargo test -p rocketmq-broker phase3_topic_config_admin_processor_returns_decodable_bodies --lib --all-features --quiet` | 1/1 通过；管理端 topic-config/mapping body 可解码 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone 消费者通过 |
| targeted Remoting `ArcMut`/`WeakArcMut`/`mut_from_ref` scan | `NO_TARGETED_ARCMUT`；Remoting production 债务为零 |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12j-remoting-protocol-after.json` | 实际 747 条；production 466/1,505、test 267/756、compatibility 14/40 occurrences |
| reviewed baseline reduction | 无 relocation approval；删除 Remoting 6 个 identity/9 occurrences 与 Broker wire-wrapper 1 occurrence，baseline 753→747、occurrences 2,311→2,301 |
| `python scripts/arc_mut_guard.py` | 通过 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` | 67/67 通过 |
| `python scripts/arc_mut_guard.py --fixtures` | 24/24 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 前两次分别捕获测试 `contains_key` lint 与 canonical identity 冗余 `.into()`，修复后通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12k 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust produce_accumulator --lib` | 27/27 通过；包含共享配置、重复 start、shutdown 期间拒绝 restart、异步 shutdown、并发容量与 deadline 合同 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 956/956，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone 消费者通过 |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12k-after.json` | 实际 744 条；production 463/1,495、test 267/756、compatibility 14/40 occurrences |
| reviewed baseline reduction | 无 relocation approval；只删除真实消失债务，baseline 747→744、occurrences 2,301→2,291 |
| `python scripts/arc_mut_guard.py` | 通过 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过；修正一次把文件路径误传给 rustfmt 的本地调用后，正式 workspace formatter 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；guard lifecycle mutex 不跨 `.await`，tracked task ownership 边界保持 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12l 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust latency --lib` | 11/11 通过；包含依赖/config 快照、8 路并发 start 单 task、同步/异步 shutdown 与 lifecycle probe |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 958/958，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| `python scripts/arc_mut_guard.py --bootstrap target/m11-12l-final-after.json` | 实际 732 条；production 454/1,481、test 264/752、compatibility 14/40 occurrences |
| reviewed baseline reduction | 无 relocation approval；只删除三个 latency 文件中的真实消失债务，baseline 744→732、occurrences 2,291→2,273 |
| `python scripts/arc_mut_guard.py` | 通过 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；detector scheduled task 继续由 Client runtime owner 跟踪，lifecycle 锁不跨网络或 shutdown await |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone 消费者通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12m 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust pull_result --lib` | 11/11 通过；包含 owned non-empty round-trip、decode/filter/offset delta 与同步 pull error path |
| `cargo test -p rocketmq-client-rust process_queue --lib` | 41/41 通过；包含 lifecycle timestamp 独立跟踪与全部 count/size/span/take/rollback/commit 合同 |
| `cargo test -p rocketmq-client-rust try_reset_pop_retry_topic --lib` | 1/1 通过；验证 clone-on-write 后队列保留值不被任务副本修改 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 960/960，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | 23 个真实删除 identity；9 个同 item service-owner fingerprint 更新逐条审核；baseline 732→709、occurrences 2,273→2,157 |
| `python scripts/arc_mut_guard.py` | 通过；Client production `ArcMut<MessageExt>` 定向扫描为零 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo clean` | 磁盘仅剩约 32 MiB 后按授权清理 192.2 GiB workspace 构建缓存；清理后完整测试重新构建并通过 |
| `cargo test -p rocketmq-admin-core --lib` / `cargo test -p rocketmq-admin-core --test boundary_source_guard` | 120/120 单测、3/3 adapter boundary tests 通过；owned PullResult 下游适配使用直接 clone/to_vec |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone 消费者通过新的 owned/standard-Arc Client API |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；消费任务 owner 未改变，ProcessQueue lifecycle metadata 只经既有异步 `RwLock` 访问 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

## 剩余切片与 Gate

1. Client MQClientInstance、Producer/Admin、消费服务 lifecycle、Push/Lite Consumer owner（120/491）。
2. Broker TopicConfig/offset、BrokerRuntimeInner、schedule/POP/processor/transaction owner（190/568）。
3. Store TopicConfig snapshot、MappedFileQueue/ConsumeQueue、CommitLog/Flush、StoreHandle/Rocks/Timer 与 HA actor（127/324）。
4. Tools 3/14、compatibility `arc_mut.rs` 和公开 re-export；移除其余 nightly feature，将 guard 切到 production/public zero。
5. 对同一候选快照执行 stable feature matrix、Miri/Loom 可用切片、soak/SLO fault、dashboard/runbook、动态
   Kind/K3d/container、M10 固定硬件和 Human Gate。

任何切片失败都只回滚对应独立 PR，不扩大 baseline，不删除 durability/fault 证据，也不把 fixture 当作动态 PASS。
