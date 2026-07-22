# M11-12 Soundness 收口进度证据

## 状态与完成边界

M11-12 的最终目标是 production/public compatibility API 中不存在 `ArcMut`、`WeakArcMut`、safe
`mut_from_ref` 或 clone-safe `AsMut`/`DerefMut`，并让默认 workspace 在 stable Rust 下通过，同时把 Miri/Loom、
soak、SLO fault、dashboard/runbook、rollback 和 Human Gate 绑定到同一候选快照。

该目标尚未完成。本文件记录 M11-12a～au 子切片的真实下降，不把子切片计为第 76 个工作包，也不刷新
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

M11-12n 的 Client consume service lifecycle 由 Issue #8321 跟踪，分支为
`mxsm/architecture-refactor-client-consume-service-lifecycle`；它仍是同一 M11-12 工作包的子切片。

M11-12o 的 Client send hook/trace context owner 由 Issue #8323 跟踪，分支为
`mxsm/architecture-refactor-client-hook-trace-boundary`；它仍是同一 M11-12 工作包的子切片。

M11-12p 的 Client Admin facade self owner 由 Issue #8325 跟踪，分支为
`mxsm/architecture-refactor-client-admin-facade-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12q 的 Client Producer fault strategy owner 由 Issue #8327 跟踪，分支为
`mxsm/architecture-refactor-client-fault-strategy-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12r 的 Client API factory owner 由 Issue #8329 跟踪，分支为
`mxsm/architecture-refactor-client-api-factory-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12s 的 Client API instance owner 由 Issue #8331 跟踪，分支为
`mxsm/architecture-refactor-client-api-instance-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12t 的 Client internal Admin owner 由 Issue #8333 跟踪，分支为
`mxsm/architecture-refactor-client-internal-admin-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12u 的 Client route registry owner 由 Issue #8335 跟踪，分支为
`mxsm/architecture-refactor-client-route-registry-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12v 的 Client OffsetStore owner 由 Issue #8337 跟踪，分支为
`mxsm/architecture-refactor-client-offset-store-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12w 的 Client accumulator batch producer owner 由 Issue #8339 跟踪，分支为
`mxsm/architecture-refactor-client-accumulator-producer-owner`；它仍是同一 M11-12 工作包的子切片。

M11-12x 的 Client remote offset read access 由 Issue #8341 跟踪，分支为
`mxsm/architecture-refactor-client-remote-offset-read-access`；它仍是同一 M11-12 工作包的子切片。

M11-12y 的 Client Push operational access 由 Issue #8343 跟踪，分支为
`mxsm/architecture-refactor-client-push-operational-access`；它仍是同一 M11-12 工作包的子切片。

M11-12z 的 Client orderly lock access 由 Issue #8345 跟踪，分支为
`mxsm/architecture-refactor-client-orderly-lock-access`；它仍是同一 M11-12 工作包的子切片。

M11-12aa 的 Client Lite Pull config snapshots 由 Issue #8347 跟踪，分支为
`mxsm/architecture-refactor-lite-pull-config-snapshots`；它仍是同一 M11-12 工作包的子切片。

M11-12ab 的 Client Lite Pull facade config snapshots 由 Issue #8349 跟踪，分支为
`mxsm/architecture-refactor-lite-pull-facade-snapshots`；它仍是同一 M11-12 工作包的子切片。

M11-12ac 的 Client Lite Pull root lifecycle 由 Issue #8351 跟踪；M11-12ad 的 PullAPIWrapper immutable access 由
Issue #8353 跟踪；M11-12ae 的 Push message listener ownership 由 Issue #8355 跟踪；M11-12af 的 Push subscription
snapshots 由 Issue #8357 跟踪；M11-12ag 的 Push consume service config snapshots 由 Issue #8359 跟踪；
M11-12ah 的 Push rebalance config snapshots 由 Issue #8361 跟踪。它们均是同一 M11-12 工作包的子切片。

M11-12ai 的 Client Push root config snapshots 由 Issue #8363 跟踪，分支为
`mxsm/architecture-refactor-push-root-config-snapshots`；它仍是同一 M11-12 工作包的子切片。

M11-12aj 的 Client Push implementation root ownership 由 Issue #8365 跟踪，分支为
`mxsm/architecture-refactor-push-root-ownership`；它仍是同一 M11-12 工作包的子切片，总目标不因该子切片完成而关闭。

M11-12ak 的 Client Rebalance root ownership 由 Issue #8367 跟踪，分支为
`mxsm/architecture-refactor-rebalance-root-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12al 的 Client MQClientInstance root ownership 由 Issue #8369 跟踪，分支为
`mxsm/architecture-refactor-mq-client-instance-root-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12am 的 Client internal child ownership 由 Issue #8371 跟踪，分支为
`mxsm/architecture-refactor-client-internal-child-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12an 的 Client Producer root ownership 由 Issue #8375 跟踪，分支为
`mxsm/architecture-refactor-client-producer-root-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12ao 的 Broker topic metadata table ownership 由 Issue #8377 跟踪，分支为
`mxsm/architecture-refactor-broker-topic-metadata-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12ap 的 Broker topic configuration ownership 由 Issue #8379 跟踪，分支为
`mxsm/architecture-refactor-broker-topic-config-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12aq 的 Broker POP buffer ownership 由 Issue #8381 跟踪，分支为
`mxsm/architecture-refactor-broker-pop-buffer-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12ar 的 Broker POP lifecycle ownership 由 Issue #8383 跟踪，分支为
`mxsm/architecture-refactor-broker-pop-lifecycle-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12as 的 Broker POP Lite lifecycle ownership 由 Issue #8385 跟踪，分支为
`mxsm/architecture-refactor-broker-pop-lite-lifecycle-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12at 的 Broker Pull lifecycle ownership 由 Issue #8387 跟踪，分支为
`mxsm/architecture-refactor-broker-pull-lifecycle-ownership`；它仍是同一 M11-12 工作包的子切片。

M11-12au 的 Broker ConsumerOffsetManager ownership 由 Issue #8389 跟踪，分支为
`mxsm/architecture-refactor-broker-consumer-offset-version`；它仍是同一 M11-12 工作包的子切片。

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

## M11-12n Client consume service lifecycle

| 目标 | 实现与证据 |
|---|---|
| 安全 service owner | `ConsumeMessageServiceGeneral`、`ConsumeMessagePopServiceGeneral` 与四类 concrete service 均由标准 `Arc` 持有；弱引用改用 `std::sync::Weak` |
| 不可变 lifecycle API | service trait 的 start/shutdown/submit API 只经 `&self`，延迟、并发与顺序任务显式捕获 `Arc<Self>`；目标范围不再出现 `ArcMut<Self>` 或 service `mut_from_ref` |
| 受控任务句柄 | Pop orderly lock-refresh handle 收入 `parking_lot::Mutex<Option<_>>`；发布与 take 在短锁内完成，shutdown 在释放 guard 后 await |
| Push 集成 | `DefaultMQPushConsumerImpl` 以 `Arc` 构造和保存通用/具体消费服务；启动、注册失败回滚、正常关闭与 direct-consume 分发无需共享可变 service owner |
| 行为合同 | concurrent/orderly、Push/Pop 的 semaphore、TaskTracker、force-stop、周期锁、自停止、retry、ack 与 offset 合同保持通过 |

M11-12n 后实际快照为 703 个条目：production 436、test 253、compatibility 14；occurrence 精确为
production 1,337、test 697、compatibility 40。相对 M11-12m 删除 4 个 production 条目/60 个 production
occurrence，以及 2 个 test 条目/23 个 test occurrence；相对初始快照累计删除 324 个 production 条目和
788 个 production occurrence。`rocketmq-client` production 债务从 120/491 降至 116/431。剩余 production
债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 116 | 431 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 从 709→703、occurrences 从 2,157→2,074。6 个 identity 来自源码真实删除；90 个旧
occurrence 消失，其中 83 个形成净下降，另有 7 个同 item、同参数的一对一 fingerprint 更新，仅因相邻 service
owner 从 `ArcMut` 改为 `Arc`、引用槽增加安全短锁或行号位移，未增加或移动共享可变 occurrence。

## M11-12o Client send hook/trace context owner

| 目标 | 实现与证据 |
|---|---|
| 异步 hook capability | 异步发送回调只快照不可变 `Arc<[Arc<dyn SendMessageHook>]>`，after-hook 不再通过 `DefaultMQProducerImpl` owner 间接执行 |
| hook context 收窄 | `SendMessageContext` 删除无业务用途的 Producer owner；公开 hook 数据只保留消息、队列、结果、错误与 trace 元数据 |
| 公开 API 迁移 | 外部 hook 不再能通过 context 取得可变 Producer lifecycle owner；需要的数据继续由既有 group/message/queue/result/exception/trace 字段提供，不引入替代 mutable facade |
| trace enrichment 收窄 | `AsyncTraceDispatcher` 只保存启动后解析出的 `client_id` 字符串，不再持有 host Producer/Consumer 实现；Consumer 不再保留从未读取的 host owner |
| 启动时序 | Producer 在 client instance 启动并确定真实 instance name 后发布 trace client host；Consumer 从已启动实现读取同一 client id，避免从 facade 配置推测 |
| 行为合同 | 定向测试验证 owner 被移除后异步 after-hook 仍执行一次，并验证 trace dispatcher 返回发布的 client host |

M11-12o 后实际快照为 698 个条目：production 432、test 252、compatibility 14；occurrence 精确为
production 1,329、test 696、compatibility 40。相对 M11-12n 删除 4 个 production 条目/8 个 production
occurrence，以及 1 个 test 条目/1 个 test occurrence；相对初始快照累计删除 328 个 production 条目和
796 个 production occurrence。`rocketmq-client` production 债务从 116/431 降至 112/423。剩余 production
债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 112 | 423 |
| `rocketmq-store` | 127 | 324 |
| `rocketmq-tools` | 3 | 14 |

reviewed baseline 从 703→698、occurrences 从 2,074→2,065。5 个 identity 和同一 Client API item 中的 1 个
occurrence 均因源码真实删除而下降；没有新增、relocation approval 或 shared-mutation 替代包装。

## M11-12p Client Admin facade self owner

| 目标 | 实现与证据 |
|---|---|
| facade 独占 owner | Client 与 admin-core 的 `DefaultMQAdminExt` 直接拥有 `DefaultMQAdminExtImpl`，`Deref`/`DerefMut`、`AsRef`/`AsMut` 与 lifecycle 委托只使用普通 Rust 引用 |
| 单一配置 owner | `ClientConfig` 由实现独占；facade 的 nameserver/TLS/config access 直接委托同一值，不再维护两个共享可变别名 |
| owner-free 注册 | `MQAdminExtInnerImpl` 收窄为空 marker；ClientInstance admin table 继续按 group 执行重复检测和注销，但不再保活或暴露完整 Admin 实现 |
| forwarding compatibility | 批量 trait 转发直接传递 `inner()`/`inner_mut()` 的普通引用，保留既有 Admin API 且不产生 shared-reference mutation |
| lifecycle 合同 | 旧“缺 self owner 必须失败”测试改为正向合同：直接拥有的 impl 无需自引用即可 start、register、shutdown，并到达 Running/ShutdownAlready |

M11-12p 后实际快照为 688 个条目：production 424、test 250、compatibility 14；occurrence 精确为
production 1,295、test 694、compatibility 40。相对 M11-12o 删除 8 个 production 条目/34 个 production
occurrence，以及 2 个 test 条目/2 个 test occurrence；相对初始快照累计删除 336 个 production 条目和
830 个 production occurrence。`rocketmq-client` production 债务从 112/423 降至 107/403，`rocketmq-tools`
production 债务从 3/14 清零。剩余 production
债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 107 | 403 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 698→688、occurrences 从 2,065→2,029。10 个 identity 和 36 个 occurrence 均因源码
真实删除而下降；没有新增、relocation approval 或替代 shared-mutation wrapper。

## M11-12q Client Producer fault strategy owner

| 目标 | 实现与证据 |
|---|---|
| Producer 独占 owner | `DefaultMQProducerImpl` 直接拥有 `MQFaultStrategy`，同步发送与配置 API 继续通过普通引用访问 |
| async send 快照 | 异步发送回调接收普通 `MQFaultStrategy` clone；延迟阈值与不可用时长表按发送时刻复制，不再传播 shared-mutation capability |
| 共享运行态 | clone 仅共享 concurrency-safe latency detector 与两个 `Arc<AtomicBool>` 运行时开关；启停和 fault update 无同步锁跨 await |
| queue filter | available/reachable filter 在选择时从共享 detector 构造轻量只读 view，不保留可变策略反向引用 |
| compatibility | Producer latency getter/setter、异步 retry queue selection、detector lifecycle 与 fault update 语义保持不变 |

M11-12q 后实际快照为 687 个条目：production 423、test 250、compatibility 14；occurrence 精确为
production 1,292、test 694、compatibility 40。相对 M11-12p 删除 1 个 production identity/3 个 production
occurrence；相对初始快照累计删除 337 个 production 条目和 833 个 production occurrence。
`rocketmq-client` production 债务从 107/403 降至 106/400。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 106 | 400 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 688→687、occurrences 从 2,029→2,026。`DefaultMQProducerImpl` 同 item 中未改动的
`MQClientInstance` 字段因相邻 fault-strategy 字段删除发生 1 条一对一 fingerprint relocation，按 ADR-013
临时审核且 approval 不提交；除此之外只删除真实消失的 3 个 occurrence，没有新增或替代 shared-mutation wrapper。

## M11-12r Client API factory owner

| 目标 | 实现与证据 |
|---|---|
| factory 标准 owner | `MQClientAPIFactory` 的 client 列表、构造/访问 API 与名称服务器周期刷新任务统一使用普通 `Arc<MQClientAPIImpl>` |
| 共享引用 capability | `MQClientAPIImpl::shutdown`、`fetch_name_server_addr` 与 `on_name_server_address_change` 收窄为 `&self`；调用方不再为 lifecycle/address 操作取得可变 owner |
| 地址缓存同步 | `name_srv_addr` 改为异步 `RwLock<Option<String>>`；相同地址的判重、remoting 地址列表发布与缓存替换在同一短写锁临界区内完成，且临界区内没有 `.await` |
| task capture | factory 周期刷新与 `MQClientInstance` 定时刷新直接捕获共享 API client handle，不再制造仅为调用 `&mut self` 的可变 clone |
| compatibility | 静态/域名 NameServer 配置、刷新任务启动/停止、重复地址判重以及 shutdown 等待合同保持不变 |

M11-12r 后实际快照为 684 个条目：production 421、test 249、compatibility 14；occurrence 精确为
production 1,286、test 693、compatibility 40。相对 M11-12q 删除 2 个 production identity/6 个 production
occurrence，并删除 1 个随 factory `ArcMut` 导入消失的 test identity/occurrence；相对初始快照累计删除 339 个
production 条目和 839 个 production occurrence。`rocketmq-client` production 债务从 106/400 降至 104/394。
剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 104 | 394 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 687→684、occurrences 从 2,026→2,019。全部 3 个 identity/7 个 occurrence 均因源码真实
删除而下降；`MQClientAPIImpl` 的既有 `ArcMut` import 因相邻新增 `RwLock` import 发生 1 条同 module 一对一
fingerprint relocation，按 ADR-013 临时审核且 approval 不提交；没有新增或替代 shared-mutation wrapper。

## M11-12s Client API instance owner

| 目标 | 实现与证据 |
|---|---|
| instance 标准 owner | `MQClientInstance` 直接构造、保存并返回 `Arc<MQClientAPIImpl>`；不再把已经 concurrency-safe 的 API client 包回 `ArcMut` |
| receiver 收窄 | `MQClientAPIImpl` 中 44 个不修改字段的 send/pull/heartbeat/admin forwarding receiver 从 `&mut self` 收窄为 `&self` |
| task capture | query/pull 静态入口接收 `Arc<Self>`，异步 pull task 只捕获普通 API owner；Admin、Producer 与 Consumer 调用链只读取 `Option<Arc<_>>` |
| escape 删除 | 两条 heartbeat 路径删除 API handle 上的 safe `mut_from_ref`，直接通过共享引用发送 heartbeat |
| compatibility | NameServer、Admin、send/pull/query、heartbeat、offset/rebalance 与 lifecycle 调用合同保持不变；accessor 类型回归断言固定为标准 `Arc` |

M11-12s 后实际快照为 683 个条目：production 420、test 249、compatibility 14；occurrence 精确为
production 1,276、test 693、compatibility 40。相对 M11-12r 删除 1 个 production identity/10 个 production
occurrence；相对初始快照累计删除 340 个 production 条目和 849 个 production occurrence。
`rocketmq-client` production 债务从 104/394 降至 103/384。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 103 | 384 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 684→683、occurrences 从 2,019→2,009。`MQClientInstance` 中未改动的 `MQAdminImpl` owner
字段因相邻 API field 从 `ArcMut` 改为 `Arc` 发生 1 条同 struct 一对一 fingerprint relocation，按 ADR-013
临时审核且 approval 不提交；除此之外只删除真实消失的 10 个 occurrence，没有新增或替代 shared-mutation wrapper。

## M11-12t Client internal Admin owner

| 目标 | 实现与证据 |
|---|---|
| Admin 标准 owner | `MQClientInstance` 直接保存 `Arc<MQAdminImpl>`，删除内部 Admin helper 的 `ArcMut` 构造与字段类型 |
| 一次绑定 | `MQAdminImpl` 的 root client handle 由 `OnceLock` 发布；`set_client(&self)` 返回是否首次绑定，release/debug 构建均执行绑定，重复绑定不替换既有 handle |
| receiver 收窄 | Admin route/query/offset/topic forwarding 方法从 `&mut self` 收窄为 `&self`；只有需要 route refresh 的调用在局部 clone 现有 root handle |
| escape 删除 | Producer 删除 11 个仅为访问 `mq_admin_impl` 的 safe `mut_from_ref`；Consumer Admin forwarding 删除冗余可变 client clone |
| compatibility | publish queue 解析、topic create、offset、query/view message 与 Consumer Admin facade 行为保持不变 |

M11-12t 后实际快照仍为 683 个条目：production 420、test 249、compatibility 14；occurrence 精确为
production 1,263、test 693、compatibility 40。相对 M11-12s 没有删除完整 identity，但真实删除 13 个 production
occurrence；相对初始快照累计删除 340 个 production 条目和 862 个 production occurrence。
`rocketmq-client` production 债务从 103/384 降至 103/371。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 103 | 371 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 条目保持 683，occurrences 从 2,009→1,996。`MQAdminImpl` 中仍存在的 3 个 root client handle
occurrence 因 `Option`→`OnceLock`、receiver 与 lookup 收窄发生同 item 一对一 fingerprint relocation，按 ADR-013
临时审核且 approval 不提交；除此之外只删除真实消失的 13 个 occurrence，没有新增或替代 shared-mutation wrapper。

## M11-12u Client route registry owner

| 目标 | 实现与证据 |
|---|---|
| route capability 收窄 | `MQClientInstance` 的 route refresh/application、route query 与 subscribe broker lookup 只经 `&self`，写入继续由既有 DashMap、原子版本和内部同步结构承担 |
| Producer 注册 | `register_producer` 收窄为 `&self`，producer table 仍以并发 map 原子登记，不扩大生命周期写权限 |
| escape 删除 | Producer 路由查找、默认路由刷新、heartbeat 与注册路径删除 4 个 safe `mut_from_ref`；production 中只保留实际调用 `MQClientInstance::start` 的 lifecycle 可变入口 |
| 调用链清理 | scheduled refresh、Admin、Push/Lite Consumer 和 proxy adapter 删除因旧 receiver 遗留的冗余可变 client clone |
| compatibility | route freshness/version guard、Producer/Consumer route view、heartbeat route index、Admin offset lookup 与 proxy adapter 行为保持不变 |

M11-12u 后实际快照仍为 683 个条目：production 420、test 249、compatibility 14；occurrence 精确为
production 1,259、test 693、compatibility 40。相对 M11-12t 没有删除完整 identity，但真实删除 4 个 production
occurrence；相对初始快照累计删除 340 个 production 条目和 866 个 production occurrence。
`rocketmq-client` production 债务从 103/371 降至 103/367。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 103 | 367 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 条目保持 683，occurrences 从 1,996→1,992。guard 只报告 Producer 中 4 个已删除的旧
occurrence，没有新增或 fingerprint relocation；因此本切片不使用 relocation approval，也没有新增或替代
shared-mutation wrapper。

## M11-12v Client OffsetStore owner

| 目标 | 实现与证据 |
|---|---|
| 标准共享 owner | Push/Lite facade、内部实现、rebalance、callback 与 offset-store API 统一改持 `Arc<OffsetStore>`，删除 `ArcMut<OffsetStore>` 构造、类型与专用 escape |
| persistence capability | `OffsetStoreTrait`、enum facade、Remote/Local backend 的 persist、persist-all 与 broker update receiver 收窄为 `&self`；offset table 与 persist command 继续经既有并发状态串行 |
| Local lifecycle | background persistence task handle 收入 `parking_lot::Mutex`；shutdown 在锁内取出 handle、释放锁后 await，重复 shutdown 保持幂等，Drop 使用独占 `get_mut` 兜底停止 |
| public API 迁移 | Push/Lite 的 offset-store getter/setter 返回/接收标准 `Arc`；这是 Phase 3 清除 production/public ArcMut 的显式编译期迁移，不保留会继续暴露旧 owner 的 compatibility wrapper |
| compatibility | offset freeze/read/persist、rebalance remove、shutdown final persist、wire 与本地 offset 文件格式保持不变 |

M11-12v 后实际快照为 681 个条目：production 418、test 249、compatibility 14；occurrence 精确为
production 1,224、test 683、compatibility 40。相对 M11-12u 真实删除 2 个 production identity、35 个 production
occurrence，以及 10 个 test occurrence；相对初始快照累计删除 342 个 production 条目和 901 个 production
occurrence。`rocketmq-client` production 债务从 103/367 降至 101/332。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 101 | 332 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 683→681、occurrences 从 1,992→1,947。`DefaultLitePullConsumerImpl.pull_api_wrapper` 因相邻
`offset_store` 字段由 `ArcMut` 改为 `Arc` 发生 1 条同 struct 同字段 fingerprint relocation，按 ADR-013 临时审核且
approval 不提交；其余变化均为真实删除，未新增或替代 shared-mutation wrapper。

## M11-12w Client accumulator batch producer owner

| 目标 | 实现与证据 |
|---|---|
| owned producer | `MessageAccumulation` 直接持有 owned `DefaultMQProducer` clone，删除 producer 外层 `ArcMut` |
| lock boundary | sync/async/guard flush 在 batch mutex 内提取消息并克隆 producer，释放 batch mutex 后才调用 `send_direct`；发送期间不持有 batch lock |
| debt 清零 | `produce_accumulator.rs` 的 production/test ArcMut constructor、type-reference 与 import identity 全部删除 |
| compatibility | aggregation key、batch message encoding、hold-size accounting、callback fan-out、failure propagation、deadline guard 与 shutdown pending-batch 语义保持不变 |

M11-12w 后实际快照为 676 个条目：production 415、test 247、compatibility 14；occurrence 精确为
production 1,219、test 676、compatibility 40。相对 M11-12v 真实删除 3 个 production identity、5 个 production
occurrence，以及 2 个 test identity、7 个 test occurrence；相对初始快照累计删除 345 个 production 条目和
906 个 production occurrence。`rocketmq-client` production 债务从 101/332 降至 98/327。剩余 production
债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 98 | 327 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 681→676、occurrences 从 1,947→1,935。guard 只报告 accumulator 中 5 个已删除 identity，
没有新增或 fingerprint relocation；因此本切片不使用 relocation approval，也没有新增或替代 shared-mutation
wrapper。

## M11-12x Client remote offset read access

| 目标 | 实现与证据 |
|---|---|
| immutable broker lookup | `RemoteBrokerOffsetStore` 直接通过共享 `MQClientInstance` 调用 `find_broker_address_in_subscribe(&self, ...)` |
| immutable route refresh | route miss 后直接调用 `update_topic_route_info_from_name_server_topic(&self, ...)`，随后按原逻辑重试 broker lookup |
| immutable API access | 直接从 immutable client instance 读取 `mq_client_api_impl`，不再制造可变别名 |
| compatibility | query header、master broker selection、route-miss retry、5 秒 timeout、not-initialized/broker-not-found 错误语义保持不变 |

M11-12x 后实际快照为 675 个条目：production 414、test 247、compatibility 14；occurrence 精确为
production 1,215、test 676、compatibility 40。相对 M11-12w 真实删除 1 个 production identity、4 个 production
occurrence；相对初始快照累计删除 346 个 production 条目和 910 个 production occurrence。
`rocketmq-client` production 债务从 98/327 降至 97/323。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 97 | 323 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 676→675、occurrences 从 1,935→1,931。guard 只报告 remote offset read path 中 1 个已删除
identity，没有新增或 fingerprint relocation；baseline 与 reviewed output 的 identity/id/fingerprint/item 语义集合
完全一致，因此本切片不使用 relocation approval，也没有新增或替代 shared-mutation wrapper。

## M11-12y Client Push operational access

| 目标 | 实现与证据 |
|---|---|
| request dispatch | Push pull/pop immediate/later dispatch receiver 收窄为 `&self`，只委托 `PullMessageService` 的 immutable capability |
| retry namespace | `reset_retry_and_namespace` 收窄为 `&self`；`ClientConfig` 新增 crate-private immutable namespace resolution，不写 lazy cache 即可保持 explicit/endpoint-derived namespace 解析 |
| POP API | `ack_async` 与 `change_pop_invisible_time_async` 收窄为 `&self`，broker lookup、route refresh、header/callback 与错误语义不变 |
| call-site closure | RebalancePush heartbeat/dispatch 与 concurrent/orderly/POP consume service 删除 9 个过时 `mut_from_ref`；rebalance lock/unlock、producer send 等真实可变路径保留 |

M11-12y 后实际快照为 672 个条目：production 411、test 247、compatibility 14；occurrence 精确为
production 1,206、test 676、compatibility 40。相对 M11-12x 真实删除 3 个 production identity、9 个 production
occurrence；相对初始快照累计删除 349 个 production 条目和 919 个 production occurrence。
`rocketmq-client` production 债务从 97/323 降至 94/314。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 94 | 314 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 675→672、occurrences 从 1,931→1,922。guard 报告 3 个完整删除 identity 和 4 个局部删除
occurrence（合计删除 9 个 occurrence），没有新增或 fingerprint relocation；baseline 与 reviewed output 的
identity/id/fingerprint/item 语义集合完全一致，因此本切片不使用 relocation approval，也没有新增或替代
shared-mutation wrapper。

## M11-12z Client orderly lock access

| 目标 | 实现与证据 |
|---|---|
| trait boundary | Rebalance 单队列 `unlock`、全队列 `lock_all`/`unlock_all` receiver 收窄为 `&self`，Push 与 Lite 实现同步收窄 |
| inner lock capability | `RebalanceImpl::lock`/`lock_with`/`lock_all`/`unlock_all` 使用 immutable receiver 和 immutable client access；仍通过 process-queue 并发表与原子状态更新锁状态/时间戳 |
| orderly call sites | orderly lock 路径不再制造 mutable alias；POP-orderly lock/unlock 同样直接调用 immutable capability，producer send 的真实可变入口保留 |
| namespace | orderly 与 POP-orderly reset 使用 `ClientConfig::resolved_namespace`，不再 clone config 后写共享 lazy-cache flag |
| compatibility | broker lookup、lock/unlock request body、oneway、periodic scheduling、queue lock state/timestamp 与 namespace stripping 语义保持不变 |

M11-12z 后实际快照为 671 个条目：production 410、test 247、compatibility 14；occurrence 精确为
production 1,203、test 676、compatibility 40。相对 M11-12y 真实删除 1 个 production identity、3 个 production
occurrence；相对初始快照累计删除 350 个 production 条目和 922 个 production occurrence。
`rocketmq-client` production 债务从 94/314 降至 93/311。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 93 | 311 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 672→671、occurrences 从 1,922→1,919。guard 报告 1 个完整删除 identity 和 2 个局部删除
occurrence（合计删除 3 个 occurrence），没有新增或 fingerprint relocation；baseline 与 reviewed output 的
identity/id/fingerprint/item 语义集合完全一致，因此本切片不使用 relocation approval，也没有新增或替代
shared-mutation wrapper。

## M11-12aa Client Lite Pull config snapshots

| 目标 | 实现与证据 |
|---|---|
| implementation config | `DefaultLitePullConsumerImpl` 的 `ClientConfig` 与 `LitePullConsumerConfig` 使用 `ArcSwap` 不可变快照；兼容 `ArcMut` 构造参数仅在边界复制，不再成为内部 owner |
| rebalance config | `RebalanceLitePullImpl` 直接持有 `ArcSwap<ConsumerConfig>`；group/model/strategy/unit/timestamp/listener 通过 copy-update-publish 同步完整代际 |
| async read boundary | start、shutdown、pull、flow control、auto-commit、hook、metadata 与 running-info 在 await 前取得不可变 `Arc` 快照，不持有同步 lock guard |
| concurrent update | 既有 consumer-group 回归测试追加四路同步配置写入，验证不同字段的并发 accepted update 不丢失 |
| compatibility | public Lite Pull facade 构造与 setter、namespace group wrapping、TLS、trace、pull、offset 与 Java-compatible getter 行为保持不变 |

M11-12aa 后实际快照仍为 671 个条目：production 410、test 247、compatibility 14；occurrence 精确为
production 1,169、test 676、compatibility 40。相对 M11-12z 没有删除完整 identity，真实删除 34 个 production
occurrence；相对初始快照累计删除 350 个 production 条目和 956 个 production occurrence。
`rocketmq-client` production 债务从 93/311 降至 93/277。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 93 | 277 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 671→671、occurrences 从 1,919→1,885。34 个 occurrence 均因内部配置共享可变入口真实删除；
11 个保留 occurrence 仅因同一函数内相邻字段/初始化变为 `ArcSwap` 而发生 fingerprint 位移，逐条按 ADR-013
临时审核且 approval 不提交。tracked baseline 与 reviewed output 的 identity/id/fingerprint/item 语义集合一致，
没有新增或替代 shared-mutation wrapper。

## M11-12ab Client Lite Pull facade config snapshots

| 目标 | 实现与证据 |
|---|---|
| facade config owner | `DefaultLitePullConsumer` 通过共享 `ArcSwap` 发布 `ClientConfig` 与 `LitePullConsumerConfig` immutable snapshot；clone facade 共享同一发布点 |
| mutation boundary | facade setter 使用 RCU copy-update-publish；并发更新不同字段不会因独立 load/store 覆盖而丢失 accepted update |
| public API | `client_config()` / `consumer_config()` 返回 owned `Arc` snapshot，consumer group 返回 owned value；primary constructor 接收 owned config，builder 不再创建 `ArcMut` |
| compatibility decision | 为满足 Phase 3 soundness Exit，直接 constructor/getter 类型有意迁移到 safe owned API；保留 `DefaultLitePullConsumer` 路径、builder、Java-compatible 方法名和运行语义，不保留会继续暴露 `ArcMut` 的签名 |
| immutable namespace | namespace/topic/queue wrapping 使用 immutable resolved namespace，不再为 lazy cache 从共享引用获取可变引用 |
| regression evidence | crate-root API 测试锁定 owned snapshot 类型；四路并发 facade setter 测试同时验证完整发布与旧 snapshot 不变 |

M11-12ab 后实际快照为 668 个条目：production 408、test 246、compatibility 14；occurrence 精确为
production 1,129、test 669、compatibility 40。相对 M11-12aa 真实删除 2 个 production identity、40 个 production
occurrence和 1 个 test identity、7 个 test occurrence；相对初始快照累计删除 352 个 production 条目和 996 个
production occurrence。`rocketmq-client` production 债务从 93/277 降至 91/237。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 91 | 237 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 671→668、occurrences 从 1,885→1,838。47 个 occurrence 真实删除；5 个 Lite Pull root
lifecycle 保留 occurrence 仅因相邻 config 字段/constructor input 改为 safe snapshot 而发生同 item fingerprint
relocation，逐条按 ADR-013 临时审核且 approval 不提交。tracked baseline 与 reviewed output 的
identity/id/fingerprint/item 语义集合 1,838/1,838 一致，没有新增或替代 shared-mutation wrapper。

## M11-12ac Client Lite Pull root lifecycle

| 目标 | 实现与证据 |
|---|---|
| root owner | facade `OnceCell`、`MQConsumerInnerImplKind::LitePull`、rebalance listener 与 metadata/pull task 使用标准 `Arc<DefaultLitePullConsumerImpl>` / `Weak`，删除 `ArcMut`/`WeakArcMut` 根 owner 和强 self-cycle |
| lifecycle transition | 专用 Tokio mutex 串行 start/shutdown、subscribe/assign/unsubscribe 与 assign-expression 控制面；start 在状态写锁内原子执行 `CreateJust→StartFailed` 预占，避免同步配置检查后被启动穿插 |
| component publication | service state、client/pull/offset/RPC/hook 与 task-handle 槽位使用短时标准锁或 `ArcSwapOption`，只在锁内 clone/take/publish，所有 I/O、task wait、offset persistence 与 client shutdown 均在锁外 await |
| child boundary | Rebalance 构造时发布 group/model/strategy 初值，subscription `DashMap` 写入和 consumer 注册收窄为 `&self`；Rebalance offset store 改为 `ArcSwapOption`，不为 root 迁移新增 `mut_from_ref` |
| regression evidence | 86 项 Lite Pull 定向测试通过；新增并发 shutdown 串行化与 weak self-reference 无保活环测试，既有 namespace/trace/pull/offset/heartbeat 行为保持 |

M11-12ac 后实际快照为 657 个条目：production 402、test 241、compatibility 14；occurrence 精确为
production 1,102、test 654、compatibility 40。相对 M11-12ab 真实删除 6 个 production identity、27 个 production
occurrence 和 5 个 test identity、15 个 test occurrence；相对初始快照累计删除 358 个 production 条目和 1,023 个
production occurrence。`rocketmq-client` crate production 债务从 91/237 降至 85/210（owner 分组为 client 84/209、
proxy 1/1）。剩余 production 债务为：

| crate | 条目 | occurrence |
|---|---:|---:|
| `rocketmq-broker` | 190 | 568 |
| `rocketmq-client` | 85 | 210 |
| `rocketmq-store` | 127 | 324 |

reviewed baseline 从 668→657、occurrences 从 1,838→1,796。42 个 occurrence 真实删除；40 个保留的 child owner 或
test fixture occurrence 仅因同 item 内 root lifecycle 字段、控制面和测试重排发生 fingerprint relocation，逐条按
ADR-013 临时审核且 approval 不提交。tracked baseline 与 reviewed output 的 identity/id/fingerprint/item 语义集合
1,796/1,796 一致，没有新增 shared-mutation occurrence 或替代 wrapper。

## M11-12ai Client Push root config snapshots

| 目标 | 实现与证据 |
|---|---|
| 单一配置发布边界 | `DefaultMQPushConsumer` 与 `DefaultMQPushConsumerImpl` 共享一个 `Arc<ArcSwap<ConsumerConfig>>`；删除根 `ArcMut<ConsumerConfig>` 字段、constructor 参数及测试 helper 构造 |
| 完整代际更新 | facade setter、trace dispatcher 初始化与 rebalance threshold 回写均 clone 当前配置、修改私有副本并单次 publish；旧 reader 的 `Arc` 快照保持不变 |
| 稳定读取 | startup/config validation、pull/pop callback、expired-message 判断与 running-info diagnostics 在逻辑操作开始时加载 immutable `Arc` 快照，不跨 await 持有同步锁 |
| 兼容边界 | Push facade/implementation 继续观察同一配置代际；consumer-group getter 改为 owned value，公开 API 不再返回可变根配置中的借用 |
| regression evidence | facade/implementation 共享 store、旧快照不变、全部 runtime setter 可见、subscription identity 与动态 threshold 原子发布定向测试通过 |

M11-12ai 后实际快照为 650 个条目：production 397、test 239、compatibility 14；occurrence 精确为 production
1,045、test 620、compatibility 40。相对 M11-12ah 删除 1 个 production identity、7 个 production occurrence
和 13 个 test occurrence；Client owner 从 80/159 降至 79/152，Client test 从 47/145 降至 47/132。
剩余 production 债务为：

| owner | 条目 | occurrence |
|---|---:|---:|
| Broker | 190 | 568 |
| Client | 79 | 152 |
| Proxy | 1 | 1 |
| Store | 127 | 324 |

reviewed baseline 从 651→650、occurrences 从 1,725→1,705。17 个外层 implementation owner/test helper
occurrence 仅因嵌套 `ConsumerConfig` owner 删除发生同 item fingerprint relocation，逐条按 ADR-013 临时审核且 approval
不提交；其余 20 个 occurrence 为真实删除。tracked baseline 与 reviewed output 的 semantic set 1,705/1,705 一致。

## M11-12aj Client Push implementation root ownership

| 目标 | 实现与证据 |
|---|---|
| 标准根 owner | `DefaultMQPushConsumer`、`MQConsumerInnerImplKind::Push`、MQClient consumer registry、pull/pop callback 与 consume request task 统一持有 `Arc<DefaultMQPushConsumerImpl>`；仓库中 `ArcMut<DefaultMQPushConsumerImpl>` 与对应 constructor 零匹配 |
| 无环回边 | root-owned 四类 consume service 与 `RebalancePushImpl` 只保存 `Weak<DefaultMQPushConsumerImpl>`；回调执行时显式 upgrade，根释放后不会由后台服务或 rebalance 回边保活 |
| 生命周期串行 | start/shutdown 通过单一异步 lifecycle mutex 串行；startup rollback 在已持锁 transition 内执行内部 shutdown，避免递归取锁；新增并发 shutdown 等待定向测试 |
| 组件快照 | service state、client instance、pull wrapper、offset store、listener、consume service 与 hook list 使用短 `RwLock` 发布/克隆快照，计数器与布尔状态使用原子；同步 guard 不跨 `await` |
| rebalance 共享访问 | `Rebalance` trait 与 Push/LitePull 实现收窄为 `&self`；初始化 metadata 使用短同步锁快照，weak self 回边使用 `OnceLock`，根迁移暴露出的 13 个 production `mut_from_ref` 全部删除 |
| regression evidence | Push root 25/25、rebalance 29/29 定向测试通过；weak back-reference 与 lifecycle serialization 各有独立测试，既有 startup rollback、route refresh 与 callback 行为保持 |

M11-12aj 后实际快照为 607 个条目：production 376、test 217、compatibility 14；occurrence 精确为 production
995、test 590、compatibility 40。相对 M11-12ai 删除 21 个 production identity/50 occurrence 与 22 个 test
identity/30 occurrence；Client owner 从 79/152 降至 58/102，Client test 从 47/132 降至 25/102。
剩余 production 债务为：

| owner | 条目 | occurrence |
|---|---:|---:|
| Broker | 190 | 568 |
| Client | 58 | 102 |
| Proxy | 1 | 1 |
| Store | 127 | 324 |

reviewed baseline 从 650→607、occurrences 从 1,705→1,625。16 个保留的 nested client/rebalance owner occurrence
只在同 path/symbol/kind/item 内因短锁快照与 `OnceLock` 发布发生 fingerprint relocation，逐条按 ADR-013 临时审核且
approval 不提交；其余 43 个 identity/80 个 occurrence 为真实删除。tracked baseline 默认 guard 已通过。下一子切片
M11-12ak 处理 Push/LitePull `Rebalance` root 的剩余 `ArcMut`/`WeakArcMut` owner；75/82 总进度不变。

## M11-12ak Client Rebalance root ownership

| 目标 | 实现与证据 |
|---|---|
| concrete root | `DefaultMQPushConsumerImpl::rebalance_impl` 与 `DefaultLitePullConsumerImpl::rebalance_impl` 从 `ArcMut` 改为标准 `Arc`，construction 与 downgrade 边界同步使用 `Arc::new`/`Arc::downgrade` |
| standard weak self-reference | `RebalanceImpl::sub_rebalance_impl`、Push/LitePull setter 与 upgrade 统一使用标准 `Weak<R>`/`Arc<R>`，删除全部 rebalance-root `WeakArcMut` import/type |
| 行为兼容 | 沿用 M11-12aj 已收窄的 `&self` Rebalance contract，不增加锁、不改变 queue assignment、offset、dispatch、startup/shutdown 或公开 consumer API |
| regression evidence | Push/LitePull 各新增一个 standard weak self-reference 回归测试；drop concrete root 后 weak upgrade 均返回 `None`，rebalance 定向测试 31/31 通过 |

M11-12ak 后实际快照为 599 个条目：production 368、test 217、compatibility 14；occurrence 精确为 production
982、test 590、compatibility 40。相对 M11-12aj 删除 8 个 production identity/13 occurrence，测试与 compatibility
均不增加；Client owner 从 58/102 降至 50/89。剩余 production 债务为：

| owner | 条目 | occurrence |
|---|---:|---:|
| Broker | 190 | 568 |
| Client | 50 | 89 |
| Proxy | 1 | 1 |
| Store | 127 | 324 |

reviewed baseline 从 607→599、occurrences 从 1,625→1,612。4 个保留的 `MQClientInstance` compatibility
occurrence 仅因相邻 Rebalance root/Weak import 删除发生同 item fingerprint relocation，逐条按 ADR-013 临时审核且
approval 不提交；其余 8 个 identity/13 个 occurrence 为真实删除。repository 中 `ArcMut<RebalancePushImpl>`、
`ArcMut<RebalanceLitePullImpl>`、对应 constructor 与 `WeakArcMut` self-reference 已零匹配。下一子切片 M11-12al
处理 `MQClientInstance` root ownership；75/82 总进度不变。

## M11-12al Client MQClientInstance root ownership

| 目标 | 实现与证据 |
|---|---|
| standard root | `MQClientManager`、Proxy compatibility handle 与 Admin/Producer/Consumer/Rebalance/API/OffsetStore 全链路统一持有标准 `Arc<MQClientInstance>`；repository 中目标 root 的 `ArcMut`/`WeakArcMut` 零匹配 |
| cycle breaking | `ClientRemotingProcessor` 与 `MQAdminImpl` 仅保存标准 `Weak<MQClientInstance>`；构造 API 借用外部 `Arc` 后 downgrade，drop root 后 weak upgrade 返回 `None` |
| lifecycle publication | `start`/`shutdown` 使用单一异步 transition mutex；service state 使用短 `RwLock`，API slot 使用 `ArcSwapOption`，connection task handle 使用短 `Mutex` 并在 await 前取出 |
| child compatibility | 尚未迁移的 DefaultProducer/PullMessageService child owner 保留在显式 lifecycle mutex 后；共享 receiver 通过 clone-local child handle 调用，不新增 production `mut_from_ref` |
| regression evidence | MQClientInstance 30/30、Remoting 22/22、Admin 7/7、Pull 10/10、Rebalance 24/24、Pull integration 28/28 与 typed-error 5/5 定向测试通过；Client 全量 library 984/984 及全部 integration targets 通过 |

M11-12al 后实际快照为 544 个条目：production 326、test 204、compatibility 14；occurrence 精确为 production
909、test 571、compatibility 40。相对 M11-12ak 删除 42 个 production identity/73 occurrence、13 个 test
identity/19 occurrence，compatibility 不增加；Client owner 从 50/89 降至 9/17，Client test 从 25/102 降至
12/83，Proxy production 从 1/1 降至零。剩余 production 债务为：

| owner | 条目 | occurrence |
|---|---:|---:|
| Broker | 190 | 568 |
| Client | 9 | 17 |
| Store | 127 | 324 |

reviewed baseline 从 599→544、occurrences 从 1,612→1,520。6 个同 identity/same item occurrence 与 2 个
test-only import identity relocation 逐条按 ADR-013 临时审核且 approval 不提交；其余为真实删除。repository 中
`ArcMut<MQClientInstance>`、`WeakArcMut<MQClientInstance>`、对应 constructor 与 root `mut_from_ref` 已零匹配。
下一子切片 M11-12am 处理 DefaultMQProducer root/Weak self owner；75/82 总进度不变。

## M11-12am Client internal child ownership

| 目标 | 实现与证据 |
|---|---|
| Pull child owner | `MQClientInstance::pull_message_service` 从 `ArcMut` 改为标准 `Arc<PullMessageService>`；其 start/shutdown/dispatch API 已是共享 receiver，内部 Clone 只共享原子与同步句柄 |
| internal producer owner | `MQClientInstance::default_producer` 从 `ArcMut` 改为 `tokio::sync::Mutex<DefaultMQProducer>`；start/shutdown/send 在同一异步 guard 内完成 |
| lifecycle equivalence | 新 owner mutex 直接替代 `default_producer_transition`；旧 transition 本来就在完整 future 上持有，因此串行范围与 `MQClientInstance lifecycle → default producer` 锁顺序均不扩大 |
| production boundary | `mq_client_instance.rs` production 不再包含 ArcMut import/type/constructor；测试所需 ArcMut 显式留在 `#[cfg(test)]` module |
| regression evidence | MQClientInstance 30/30、Pull service 10/10、Pull integration 28/28 与 typed-error 5/5 定向测试通过；Client 全量 library 984/984 及全部 integration targets 通过 |

M11-12am 后实际快照为 541 个条目：production 323、test 204、compatibility 14；occurrence 精确为 production
904、test 571、compatibility 40。相对 M11-12al 删除 3 个 production identity/5 occurrence，test 与 compatibility
不增加；Client owner 从 9/17 降至 6/12，Client test 保持 12/83。剩余 production 债务为：

| owner | 条目 | occurrence |
|---|---:|---:|
| Broker | 190 | 568 |
| Client | 6 | 12 |
| Store | 127 | 324 |

reviewed baseline 从 544→541、occurrences 从 1,520→1,515。1 个 test-only import identity relocation 按
ADR-013 临时审核且 approval 不提交；3 个 production identity/5 occurrence 为真实删除。下一子切片 M11-12an
必须一次完成 facade/implementation/registry 的标准 Arc/Weak、config snapshot、task admission 与 Client/fault
resolver 强环拆除，不能用全局 impl mutex 串行发送；75/82 总进度不变。

## M11-12an Client Producer root ownership

| 目标 | 实现与证据 |
|---|---|
| standard root 与 registry | `DefaultMQProducer` facade 统一持有标准 `Arc<DefaultMQProducerImpl>`，self-reference、Client、resolver 与 detector 回边统一为标准 `Weak`；`MQProducerInnerImpl` registry 只持 weak owner，dead entry 可安全裁剪和原子替换 |
| coherent configuration | implementation 以单一 `ArcSwap<ProducerRuntimeSnapshot>` 同时发布 Client/Producer/send 配置，所有写入由短 `config_update` 锁串行；发送、重试和 kernel 路径传递同一 runtime snapshot，不使用全局 impl mutex 串行发送 |
| facade clone compatibility | clone-shared append-only immutable `StableConfig` 从最新完整代际 copy-update-publish，消除 clone 间 lost update；旧代际只追加不修改或删除，保留 `&ProducerConfig`/`&[u64]` 借用 API，旧 slice 在另一 clone 连续更新 128 次后仍有效 |
| lifecycle 与 task admission | Tokio lifecycle mutex 串行 start/shutdown，取消后的 `Starting`/`Stopping` 可恢复清理；启动配置在 config lock 下重新加载，所有异步初始化完成后才发布 `Running`；task admission 与 tracker close 在同一短锁内排序 |
| owner-aware cleanup | producer registration admission 与 shutdown 关闭同序，独立异步 registry transition 将 table identity check/removal、broker unregister 与 replacement registration 排序；迟到的旧 owner cleanup 使用 identity-aware `remove_if`，不会删除同 group replacement，dead callback 返回空/false/no-op |
| compatibility 决策 | 公开 getter carrier 从 `Option<&ArcMut<DefaultMQProducerImpl>>` 迁移为 `Option<&Arc<DefaultMQProducerImpl>>`，显式类型标注和 `mut_from_ref` 调用需要迁移；这是 production unsafe capability 清零所需的有意 source break，public API compile test 固定新的标准 Arc carrier，其余 producer/transaction/hook/request/reply/retry/timeout/namespace/shutdown 行为保持 |
| regression evidence | Producer implementation focused tests 47/47；Client all-features library 1,002/1,002、全部 integration targets 与 doc tests 通过；共享 facade config、稳定借用、Weak drop、dead replacement、迟到 cleanup、registry transition、启动取消、Running 发布顺序与 task admission 均有确定性覆盖 |

M11-12an 后实际快照为 527 个条目：production 317、test 196、compatibility 14；occurrence 精确为 production
892、test 559、compatibility 40。相对 M11-12am 删除 6 个 production identity/12 occurrence 与 8 个 test
identity/12 occurrence；Client production 从 6/12 清零，Client test 从 12/83 降至 4/71。剩余 production 债务为：

| owner | 条目 | occurrence |
|---|---:|---:|
| Broker | 190 | 568 |
| Store | 127 | 324 |

reviewed baseline 从 541→527、occurrences 从 1,515→1,491；全部 14 个 identity/24 个 occurrence 都是源码真实删除，
无 identity/occurrence relocation，因此不需要 ADR-013 临时 approval。下一子切片 M11-12ao 进入 Broker owner 收口；
75/82 总进度不变。

## M11-12ao Broker topic metadata table ownership

| 目标 | 实现与证据 |
|---|---|
| route table owner | `TopicRouteInfoManager` 的 route、broker-address、publish-route 和 subscribe-queue 四张表从 `ArcMut<HashMap>` 改为标准 `Arc<DashMap>`；manager clone 仍共享表 owner，不再暴露共享裸可变引用 |
| guard 与异步边界 | route/subscription/publish 查询先克隆完整值并释放 DashMap guard；同表 insert/remove 和 NameServer 异步刷新前均不保留 guard，不引入同步锁跨 `.await` |
| immutable mapping generations | `TopicQueueMappingManager` 表项从 `ArcMut<TopicQueueMappingDetail>` 改为标准 `Arc`；update、decode 与 cleanup 构造完整新值后替换，cleanup 以 observed Arc identity 做条件发布，代际已变化时拒绝 stale replacement；旧 `Arc` 代际保持原内容且不会被原地修改 |
| compatibility 边界 | 两个 manager 均为 Broker 内部 owner，getter carrier 仅在 crate 内从 legacy wrapper 迁移到标准 `Arc`；wire DTO、序列化字段、数据版本递增、静态 topic epoch/broker/expected-items 校验和最终一致语义保持不变 |
| regression evidence | route manager clone 共享/route cleanup、queue mapping 13 项 lookup/update/decode/expected-version cleanup 测试通过；replacement 测试显式证明旧 mapping generation 在新值发布后仍有效，stale cleanup 测试证明并发新代际不会被覆盖 |

M11-12ao 后实际快照为 520 个条目：production 312、test 194、compatibility 14；occurrence 精确为 production
873、test 551、compatibility 40。相对 M11-12an 删除 5 个 production identity/19 occurrence 与 2 个 test
identity/8 occurrence；Broker production 从 190/568 降至 185/549。剩余 production 债务为：

| owner | 条目 | occurrence |
|---|---:|---:|
| Broker | 185 | 549 |
| Store | 127 | 324 |

reviewed baseline 从 527→520、occurrences 从 1,491→1,464；全部 7 个 identity/27 个 occurrence 都是源码真实删除，
无 identity/occurrence relocation，因此不需要 ADR-013 临时 approval。下一子切片 M11-12ap 处理 Broker
TopicConfig value/DataVersion ownership；75/82 总进度不变。

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

M11-12n 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust consume_message_ --lib` | 83/83 通过；覆盖四类 service lifecycle、任务 shutdown/force-stop、周期锁、自停止、retry/ack/offset 以及 late self-reference refresh |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 961/961，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | 6 个真实删除 identity；90 个旧 occurrence 消失、7 个同 item fingerprint 更新逐条审核，净下降 83；baseline 709→703、occurrences 2,157→2,074 |
| `python scripts/arc_mut_guard.py` | 通过；consume service owner 定向扫描为零，Client production 降至 116/431 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；task ownership 改用 `Arc<Self>`，Pop orderly handle 与 concurrent self-reference 槽均在 await 前释放短锁 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone consumer 通过新的标准 `Arc` consume-service API |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12o 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust async_send_after_hook_uses_immutable_hook_snapshot_without_producer_owner --lib` | 1/1 通过；after-hook 仅经不可变 hook 快照执行 |
| `cargo test -p rocketmq-client-rust test_set_client_host --lib` | 1/1 通过；trace client host 只保存值快照 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 962/962，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | 无 relocation approval；删除 5 个 identity 和 9 个真实 occurrence，baseline 703→698、occurrences 2,074→2,065 |
| `python scripts/arc_mut_guard.py` | 通过；发送 hook/trace context owner 定向扫描为零，Client production 降至 112/423 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone consumer 通过收窄后的 hook/trace API |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；异步回调只捕获不可变 hook 快照，未新增 detached task 或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `git diff --check` | 通过 |

M11-12p 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust directly_owned_impl_starts_and_stops_without_self_reference --lib` | 1/1 通过；直接 owned impl 无需 self reference 即可启动和关闭 |
| `cargo test -p rocketmq-client-rust admin::default_mq_admin_ext --lib` | 69/69 通过；facade/config/lifecycle 与全部 Admin helper 合同通过 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 962/962，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| `cargo test -p rocketmq-admin-core --lib --test legacy_surface_compile --test boundary_source_guard` | 120/120 library、2/2 legacy surface、3/3 boundary tests 通过 |
| reviewed baseline reduction | 无 relocation approval；删除 10 个 identity 和 36 个真实 occurrence，baseline 698→688、occurrences 2,065→2,029 |
| `python scripts/arc_mut_guard.py` | 通过；Admin facade/config/registration 自引用 owner 定向扫描为零，Client production 降至 107/403、Tools production 清零 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone consumer 通过直接 owned Admin facade API |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；Admin lifecycle owner 改造未新增 detached task、运行时或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Admin self-owner 定向扫描 | 零匹配；`ArcMut<ClientConfig>`、`ArcMut<DefaultMQAdminExtImpl>`、`set_inner` 与冗余 inner `as_ref`/`as_mut` 均已删除 |
| `git diff --check` | 通过 |

M11-12q 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust fault_strategy --lib` | 3/3 通过；clone 共享运行时开关并保留发送时阈值快照 |
| `cargo test -p rocketmq-client-rust producer_java_facade_accessors_sync_impl_without_panic --lib` | 1/1 通过；Producer latency getter/setter 合同保持 |
| `cargo test -p rocketmq-client-rust async_retry_queue --lib` | 2/2 通过；异步 retry 队列选择保持 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 全部通过；library 963/963，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction（临时 ADR-013 approval） | 仅批准 1 条同 item 相邻字段 fingerprint relocation，删除 1 个 identity/3 个 occurrence；baseline 688→687、occurrences 2,029→2,026；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；`ArcMut<MQFaultStrategy>` 定向扫描为零，Client production 降至 106/400 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone Producer 使用新的 owned/snapshot fault strategy API 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；async send 只捕获普通策略快照，未新增 detached task、运行时或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `ArcMut<MQFaultStrategy>` 定向扫描 | 零匹配 |
| `git diff --check` | 通过 |

M11-12r 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过 |
| `cargo test -p rocketmq-client-rust name_server_cache_serializes_shared_updates --lib` | 1/1 通过；并发重复地址只发布一次，后续新地址正常替换 |
| `cargo test -p rocketmq-client-rust implementation::mq_client_api_factory --lib` | 11/11 通过；静态/域名配置、刷新任务启动/停止与 shutdown 等待合同保持 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 重跑以退出码 0 全部通过；library 964/964，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction（临时 ADR-013 approval） | 仅批准 1 条同 module 相邻 import fingerprint relocation，删除 3 个 identity/7 个 occurrence；baseline 687→684、occurrences 2,026→2,019；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；factory shared-mutation owner 与 refresh mutable clone 定向扫描均为零，Client production 降至 104/394 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone Client API consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；刷新任务只捕获共享 API client handle，地址缓存临界区无 `.await`，未新增 detached task 或运行时 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| factory shared-mutation owner / refresh mutable clone 定向扫描 | 均零匹配 |
| `git diff --check` | 通过 |

M11-12s 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；API handle/receiver 全调用链类型收敛且无 unused-mut warning |
| `cargo test -p rocketmq-client-rust mq_client_api_impl_inherits_tls_flag_from_client_config --lib` | 1/1 通过；显式类型断言固定 accessor 返回 `Arc<MQClientAPIImpl>`，TLS 配置传播保持 |
| `cargo test -p rocketmq-client-rust implementation::mq_client_api_factory --lib` | 11/11 通过；factory 生命周期与 NameServer 配置合同保持 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 964/964，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction（临时 ADR-013 approval） | 仅批准 1 条同 struct 相邻字段 fingerprint relocation，删除 1 个 identity/10 个 occurrence；baseline 684→683、occurrences 2,019→2,009；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；Client production 降至 103/384 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone Client/Admin consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；query/pull task 只捕获普通 `Arc` API owner，未新增 detached task、运行时或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `ArcMut<MQClientAPIImpl>` / API forwarding `&mut self` / API heartbeat `mut_from_ref` 定向扫描 | 均零匹配 |
| `git diff --check` | 通过 |

M11-12t 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Admin/Producer/Consumer 调用链类型收敛且无 unused-mut warning |
| `cargo test -p rocketmq-client-rust implementation::mq_admin_impl --lib` | 7/7 通过；包含一次 client 绑定、重复绑定拒绝和既有 query/timestamp 合同 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 965/965，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction（临时 ADR-013 approval） | 仅批准 3 条同 item root client handle fingerprint relocation，删除 13 个 occurrence；baseline 条目保持 683、occurrences 2,009→1,996；approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；Client production occurrence 降至 371 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone Client/Admin consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；一次绑定不产生后台任务，Admin forwarding 未新增 detached task、运行时或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `ArcMut<MQAdminImpl>` / Admin `&mut self` / Producer Admin-only `mut_from_ref` 定向扫描 | 均零匹配 |
| `git diff --check` | 通过 |

M11-12u 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；route registry、Producer、Admin、Push/Lite Consumer 与 proxy adapter 调用链类型收敛且无 unused-mut warning |
| `cargo test -p rocketmq-client-rust factory::mq_client_instance::tests --lib --all-features` | 29/29 通过；覆盖 Producer 注册、并发 stale guard、Producer/Consumer route view、heartbeat route index、缓存查询与 lifecycle rollback |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 965/965，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | guard 只报告 4 个已删除旧 occurrence，无新增和 relocation；baseline 条目保持 683、occurrences 1,996→1,992 |
| `python scripts/arc_mut_guard.py` | 通过；production 420/1,259，Client 103/367 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone Client/Admin consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；route refresh scheduled task 只捕获共享 owner，未新增 detached task、运行时或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Producer production `mut_from_ref` / route-registry `&mut self` 定向扫描 | Producer 仅余 1 个真实 lifecycle start；本切片收窄的 route/registration 方法为零匹配 |
| `git diff --check` | 通过 |

M11-12v 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Push/Lite facade、实现、rebalance、callback 与 Local/Remote backend 全部完成标准 Arc 类型收敛 |
| `cargo test -p rocketmq-client-rust offset_store --lib --all-features` | 12/12 通过；覆盖 Push/Lite 注入、Remote persist coalescing、Local final persist/timeout/no-runtime/repeated-shutdown lifecycle |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 965/965，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction（临时 ADR-013 approval） | 仅批准 1 条同 struct 同字段 `pull_api_wrapper` fingerprint relocation；删除 2 个 identity、45 个 occurrence；baseline 683/1,992→681/1,947，approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；production 418/1,224，Client 101/332 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone Client consumer 通过新的标准 Arc OffsetStore API |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；Local task handle 在锁内取出、锁外 await，未新增 detached task、运行时或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| `ArcMut<OffsetStore>` / offset-store `mut_from_ref` / persistence `&mut self` 定向扫描 | 均零匹配；baseline 与 reviewed output 的 identity/id/fingerprint/item 语义集合一致 |
| `git diff --check` | 通过 |

M11-12w 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；accumulator sync/async/guard flush 使用 owned producer clone 且无可变性 warning |
| `cargo test -p rocketmq-client-rust producer::produce_accumulator --lib --all-features` | 25/25 通过；覆盖 batch 构造/关闭竞争、sync guard、callback/hold-size 回收、shutdown 与 task lifecycle |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 965/965，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | guard 只报告 5 个已删除 identity，无新增和 relocation；baseline 681/1,947→676/1,935 |
| `python scripts/arc_mut_guard.py` | 通过；production 415/1,219，Client 98/327 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone producer/consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；batch mutex 在 send await 前释放，未新增 detached task、运行时或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| accumulator `ArcMut` / `WeakArcMut` / `mut_from_ref` 定向扫描 | production/test 均零匹配；baseline 与 reviewed output 的 identity/id/fingerprint/item 语义集合一致 |
| `git diff --check` | 通过 |

M11-12x 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；remote offset broker lookup、route refresh 与 client API 读取使用 immutable access |
| `cargo test -p rocketmq-client-rust consumer::store::remote_broker_offset_store --lib --all-features` | 2/2 通过；persist coalescing 与 removal state 语义保持不变 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 965/965，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | guard 只报告 1 个已删除 identity、4 个 occurrence，无新增和 relocation；baseline 676/1,935→675/1,931 |
| `python scripts/arc_mut_guard.py` | 通过；production 414/1,215，Client 97/323 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone producer/consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；未新增 task/runtime/blocking 边界，offset query 只读路径不持有同步 guard 跨 await |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| remote offset `mut_from_ref` 定向扫描 | 零匹配；baseline 与 reviewed output 的 identity/id/fingerprint/item 语义集合一致 |
| `git diff --check` | 通过 |

M11-12y 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Push request/retry/POP operational receiver 使用 immutable access |
| `cargo test -p rocketmq-client-rust base::client_config::tests --lib --all-features` | 5/5 通过；覆盖 immutable namespace resolution 与既有 namespace collection helper |
| `cargo test -p rocketmq-client-rust consumer::consumer_impl::default_mq_push_consumer_impl::tests --lib --all-features` | 21/21 通过；覆盖 pull/pop dispatch、retry、startup rollback 与 running-info 行为 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 966/966，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | guard 报告 3 个完整删除 identity、4 个局部删除 occurrence，合计删除 9 个 occurrence；无新增和 relocation；baseline 675/1,931→672/1,922 |
| `python scripts/arc_mut_guard.py` | 通过；production 411/1,206，Client 94/314 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone producer/consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；未新增 task/runtime/blocking 边界，immutable route/API path 不持有同步 guard 跨 await |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Push operational `mut_from_ref` 定向扫描 | RebalancePush 零匹配；consume paths 只保留 rebalance lock/unlock 与 producer send 等真实可变入口，baseline 与 reviewed 语义集合一致 |
| `git diff --check` | 通过 |

M11-12z 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Rebalance lock/unlock trait、Push/Lite/inner 实现与 orderly call sites 使用 immutable receiver |
| `cargo test -p rocketmq-client-rust consumer::consumer_impl::consume_message_orderly_service::tests --lib --all-features` | 14/14 通过；覆盖 lock path、namespace reset、periodic lifecycle 与 shutdown |
| `cargo test -p rocketmq-client-rust consumer::consumer_impl::consume_message_pop_orderly_service::tests --lib --all-features` | 17/17 通过；覆盖 lock refresh、namespace reset、task lifecycle、shutdown 与 retry bounds |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 966/966，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction | guard 报告 1 个完整删除 identity、2 个局部删除 occurrence，合计删除 3 个 occurrence；无新增和 relocation；baseline 672/1,922→671/1,919 |
| `python scripts/arc_mut_guard.py` | 通过；production 410/1,203，Client 93/311 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| `rocketmq-example`: `cargo fmt --all -- --check` / `cargo clippy --all-targets -- -D warnings` | standalone producer/consumer 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；未新增 task/runtime/blocking 边界，periodic lock lifecycle 与 await 边界保持受控 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| orderly lock `mut_from_ref` 定向扫描 | orderly 零匹配；POP-orderly 仅保留 producer send，baseline 与 reviewed output 语义集合一致 |
| `git diff --check` | 通过 |

M11-12aa 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Lite Pull implementation/rebalance config 使用 `ArcSwap` snapshot，全部 target 编译通过 |
| `cargo test -p rocketmq-client-rust default_lite_pull_consumer --lib` | 83/83 通过；覆盖 config setter 同步、并发 copy-update-publish、namespace、trace、pull、offset、rebalance 与 lifecycle |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 966/966，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction（临时 ADR-013 approval） | 34 个 production occurrence 真实删除；11 个同 item 保留 occurrence 的 fingerprint relocation 逐条审核，approval 不提交；baseline 671/1,919→671/1,885 |
| `python scripts/arc_mut_guard.py` | 通过；production 410/1,169，Client 93/277，tracked/reviewed semantic set 1,885/1,885 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；异步路径只持有 immutable `Arc` config snapshot，不跨 await 持有同步 lock guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Lite Pull internal config `mut_from_ref` 定向扫描 | implementation 与 rebalance config 零匹配；兼容 facade/root lifecycle 边界保留给后续切片 |
| `git diff --check` | 通过 |

M11-12ab 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；safe facade constructor/getter snapshot API 与全部 target 编译通过 |
| `cargo test -p rocketmq-client-rust default_lite_pull_consumer --lib` | 84/84 通过；新增四路并发 facade setter 与 immutable old-snapshot 回归 |
| `cargo test -p rocketmq-client-rust --test public_api_exports_test crate_root_exports_modern_client_facades_and_traits` | 1/1 通过；crate-root facade getter 锁定为 owned `Arc` snapshot，consumer group 为 owned value |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 967/967，其余 integration targets 全部通过（35 项既有外部环境测试忽略） |
| reviewed baseline reduction（临时 ADR-013 approval） | 2 个 production identity/40 个 production occurrence 与 1 个 test identity/7 个 test occurrence 真实删除；5 个 Lite Pull root owner 同 item relocation 逐条审核，approval 不提交；baseline 671/1,885→668/1,838 |
| `python scripts/arc_mut_guard.py` | 通过；production 408/1,129，Client 91/237，tracked/reviewed semantic set 1,838/1,838 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；facade async 路径不跨 await 持有同步 guard，未增加 task/runtime/blocking 边界 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates |
| `.\scripts\check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Lite Pull config ArcMut 定向扫描 | facade、builder、implementation constructor 的 config ArcMut/type/getter/`mut_from_ref` 零匹配；仅保留 root lifecycle owner 给 M11-12ac |
| `git diff --check` | 通过 |

M11-12ac 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；标准 `Arc` root、shared receiver 与全部 test/integration target 编译通过 |
| `cargo test -p rocketmq-client-rust default_lite_pull_consumer --lib --all-features` | 86/86 通过；新增 concurrent shutdown lifecycle 与 weak-root 无保活环回归 |
| `cargo test -p rocketmq-client-rust --test public_api_exports_test crate_root_exports_modern_client_facades_and_traits --all-features` | 1/1 通过；crate-root Lite Pull facade 与 owned config snapshot API 保持 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 969/969，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction（临时 ADR-013 approval） | 6 个 production identity/27 个 production occurrence 与 5 个 test identity/15 个 test occurrence 真实删除；40 个同 item relocation 逐条审核，未配对新增为零；baseline 668/1,838→657/1,796 |
| `python scripts/arc_mut_guard.py` | 通过；production 402/1,102，Client crate 85/210，tracked/reviewed semantic set 1,796/1,796 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；同步槽位仅短时 clone/take/publish，lifecycle Tokio mutex 是显式 control-plane transition owner |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Lite Pull root ArcMut 定向扫描 | `ArcMut<DefaultLitePullConsumerImpl>`、`WeakArcMut<DefaultLitePullConsumerImpl>`、root constructor 与 root `mut_from_ref` 零匹配；child Rebalance/Pull/MQClient owner 保留给后续切片 |
| `git diff --check` | 通过 |

M11-12ad 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Lite/Push wrapper 使用标准 `Arc`，pull/POP/filter-server 全部 target 编译通过 |
| `cargo test -p rocketmq-client-rust pull_api_wrapper --lib --all-features` | 9/9 通过；覆盖共享原子运行参数、hook 整代替换、tag/SQL/class-filter/inner-batch 与 offset metadata |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 971/971，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction（临时 ADR-013 approval） | 7 个 production/Client occurrence 与 1 个 test occurrence 真实删除；2 个同 item relocation 逐条审核，approval 不提交；baseline 657/1,796→657/1,788 |
| `python scripts/arc_mut_guard.py` | 通过；production 402/1,095，Client 85/203，tracked/reviewed semantic set 1,788/1,788 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；wrapper 只共享 immutable `Arc`，原子/`ArcSwap` 操作不跨 await 持有 guard，未新增 task/runtime/blocking 边界 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| PullAPIWrapper ArcMut 定向扫描 | `ArcMut<PullAPIWrapper>` 与 wrapper setting `mut_from_ref` 零匹配；MQClientInstance child owner 保留给后续切片 |
| `git diff --check` | 通过 |

M11-12ae 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Push listener 的标准 `Arc` public/internal handle 与全部 target 编译通过 |
| `cargo test -p rocketmq-client-rust push_message_listener --lib --all-features` | 1/1 通过；覆盖 Arc 身份保持、facade/config/implementation 同步、concurrent→orderly 替换与清除 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 971/971，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction | 9 个 production/Client occurrence 与 3 个 test occurrence 真实删除，1 个 test identity 删除；无新增和 relocation；baseline 657/1,788→656/1,776 |
| `python scripts/arc_mut_guard.py` | 通过；production 402/1,086，Client 85/194，tracked/reviewed semantic set 1,776/1,776 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；listener handle 仅为 immutable `Arc`，未新增 task/runtime/blocking 边界 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Push MessageListener ArcMut 定向扫描 | `ArcMut<MessageListener>` 与对应 constructor 零匹配；Push config/root 其余 owner 保留给后续切片 |
| `git diff --check` | 通过 |

M11-12af 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Push subscription immutable snapshot、Lite compatibility conversion 与全部 target 编译通过 |
| Push subscription focused tests | facade snapshot identity/旧代际不变与 implementation startup copy 各 1/1 通过 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 973/973，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction | 2 个 production/Client identity/8 occurrence 与 1 个 test identity/1 occurrence 真实删除；无新增和 relocation；baseline 656/1,776→653/1,767 |
| `python scripts/arc_mut_guard.py` | 通过；production 400/1,078，Client 83/186，tracked/reviewed semantic set 1,767/1,767 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；startup snapshot 只读 clone/iterate，不跨 await 持有同步 guard，未新增 task/runtime/blocking 边界 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Push subscription ArcMut 定向扫描 | `ArcMut<HashMap<CheetahString, CheetahString>>` 与 subscription constructor 零匹配；dynamic rebalance subscription table 保持独立 |
| `git diff --check` | 通过 |

M11-12ag 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；四类 Push consume service 的 immutable config snapshot 与全部 target 编译通过 |
| Push consume service config focused tests | 同一启动代 `Arc` identity 1/1、orderly/POP-orderly namespace snapshot 2/2 通过 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 974/974，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction（临时 ADR-013 approval） | 2 个 production identity/24 occurrence 与 16 个 test occurrence 真实删除；4 个同 path/symbol/kind/item 的保留 occurrence relocation 逐条审核，approval 不提交；baseline 653/1,767→651/1,727 |
| `python scripts/arc_mut_guard.py` | 通过；production 398/1,054，Client owner 80/161，Proxy 1/1，tracked/reviewed semantic set 1,727/1,727 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；启动快照在异步服务创建前完成，未新增 task/runtime/blocking 边界或跨 await 同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Push service config ArcMut 定向扫描 | 四个服务的 `ArcMut<ClientConfig>`/`ArcMut<ConsumerConfig>` 字段与 constructor 参数零匹配；实时 Push implementation owner 保留给后续切片 |
| `git diff --check` | 通过 |

M11-12ah 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；RebalancePush `ArcSwap` config generation、facade 同步与全部 target 编译通过 |
| Push rebalance config focused tests | immutable 初始代际、threshold 重算、listener 新代际、owner threshold 回写与 facade setter 同步共 5/5 通过 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 978/978，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction（临时 ADR-013 approval） | 2 个 production/Client occurrence 真实删除；2 个同 path/symbol/kind/item 的保留 occurrence relocation 逐条审核，approval 不提交；baseline 651/1,727→651/1,725 |
| `python scripts/arc_mut_guard.py` | 通过；production 398/1,052，Client owner 80/159，Proxy 1/1，tracked/reviewed semantic set 1,725/1,725 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；ArcSwap 只发布 owned generation，异步 rebalance 路径持有 owned `Arc` 而非同步 guard |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Push rebalance config ArcMut 定向扫描 | `RebalancePushImpl` 中 `ArcMut<ConsumerConfig>` 字段与 constructor 参数零匹配；Push root、MQClientInstance 与 Rebalance owner 保留给后续切片 |
| `git diff --check` | 通过 |

M11-12ai 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Push root `ArcSwap` config store、owned constructor boundary 与全部 target 编译通过 |
| Push root config focused tests | facade/implementation 同一 store、旧 snapshot 隔离、runtime setter 发布、subscription identity 与 rebalance threshold 原子更新共 3/3 定向命令通过 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 978/978，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction（临时 ADR-013 approval） | 1 个 production identity/7 occurrence 与 13 个 test occurrence 真实删除；17 个同 path/symbol/kind/item 的外层 implementation owner relocation 逐条审核，approval 不提交；baseline 651/1,725→650/1,705 |
| `python scripts/arc_mut_guard.py` | 通过；production 397/1,045，Client owner 79/152，Client test 47/132，Proxy 1/1，tracked/reviewed semantic set 1,705/1,705 一致 |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；ArcSwap 发布 owned generation，异步路径只持有 immutable `Arc`，未新增 task/runtime/blocking 边界 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，guard 单测 49/49 通过 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Push root ConsumerConfig ArcMut 定向扫描 | facade/implementation 字段、constructor 与测试 helper 的 `ArcMut<ConsumerConfig>` / `ArcMut::new(ConsumerConfig)` 零匹配 |
| `git diff --check` | 通过 |

M11-12aj 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；标准 Push root、共享引用 rebalance contract 与全部 target 编译通过 |
| Push root / rebalance focused tests | `default_mq_push_consumer_impl::tests` 25/25、`consumer_impl::re_balance` 29/29 通过；包含 weak back-reference 与 lifecycle serialization 定向测试 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 980/980，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction（临时 ADR-013 approval） | 21 个 production identity/50 occurrence 与 22 个 test identity/30 occurrence 真实删除；16 个同 path/symbol/kind/item 的 nested client/rebalance owner relocation 逐条审核，approval 不提交；baseline 650/1,705→607/1,625 |
| `python scripts/arc_mut_guard.py` | 通过；production 376/995，Client owner 58/102，Client test 25/102，Proxy 1/1；没有新增 production `mut_from_ref` |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；lifecycle transition 串行，短同步锁只发布/克隆 owned handle，异步路径锁外工作 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，guard 单测 49/49 通过 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Push implementation root ArcMut 定向扫描 | repository 中 `ArcMut<DefaultMQPushConsumerImpl>` / 对应 constructor 零匹配；root production `mut_from_ref` 零匹配 |
| `git diff --check` | 通过；仅报告工作树 CRLF 转换提示，无 whitespace error |

M11-12ak 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；Push/LitePull concrete Rebalance standard `Arc` root 与全部 Client target 编译通过 |
| `cargo test -p rocketmq-client-rust --lib consumer_impl::re_balance --all-features --quiet` | 31/31 通过；包含 Push/LitePull standard weak self-reference drop 回归测试 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 982/982，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction（临时 ADR-013 approval） | 8 个 production identity/13 occurrence 真实删除；4 个保留的 `MQClientInstance` occurrence relocation 逐条审核，approval 不提交；baseline 607/1,625→599/1,612 |
| `python scripts/arc_mut_guard.py` | 通过；production 368/982，Client owner 50/89，Client test 25/102，Proxy 1/1；没有新增 production `mut_from_ref` |
| `python -m unittest scripts.tests.test_arc_mut_guard` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；本切片仅替换标准 `Arc`/`Weak` root/back-reference，未增加 runtime ownership 例外 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，guard 单测 49/49 通过 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Rebalance root ArcMut 定向扫描 | repository 中 Push/LitePull concrete Rebalance `ArcMut`/constructor 与 `WeakArcMut` self-reference 零匹配 |
| `git diff --check` | 通过；仅报告工作树 CRLF 转换提示，无 whitespace error |

M11-12al 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；标准 MQClientInstance root 与所有 Client target 编译通过 |
| MQClientInstance/Remoting/Admin/Pull/Rebalance focused tests | 30/30、22/22、7/7、10/10、24/24 通过；Pull integration 28/28、typed-error 5/5 通过 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 984/984，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline promotion（临时 ADR-013 approval） | 42 个 production identity/73 occurrence、13 个 test identity/19 occurrence 净删除；6 个同 identity occurrence 与 2 个 test import identity relocation 逐条审核，approval 不提交；baseline 599/1,612→544/1,520 |
| `python scripts/arc_mut_guard.py` | 通过；production 326/909、test 204/571、compatibility 14/40；Client owner 9/17、Client test 12/83，Proxy production 清零；没有新增 production `mut_from_ref` |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --no-deps --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；root、Pull/Rebalance service 与 default producer transition 串行，短同步锁不跨 `.await` |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，guard 单测 49/49 通过 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| MQClientInstance root ArcMut 定向扫描 | repository 中 `ArcMut<MQClientInstance>` / `WeakArcMut<MQClientInstance>` / 对应 constructor 零匹配；root 文件无 `mut_from_ref` |
| `git diff --check` | 通过；仅报告工作树 CRLF 转换提示，无 whitespace error |

M11-12am 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；标准 PullMessageService child 与 internal producer mutex owner 在所有 Client target 编译通过 |
| MQClientInstance / PullMessageService focused tests | 30/30、10/10 通过；Pull integration 28/28、typed-error 5/5 通过 |
| `cargo test -p rocketmq-client-rust --all-features --quiet` | 退出码 0 全部通过；library 984/984，其余 integration targets 全部通过，35 项既有外部环境测试忽略 |
| reviewed baseline promotion（临时 ADR-013 approval） | 删除 3 个 production identity/5 occurrence；1 个 test import identity relocation 逐条审核且 approval 不提交；baseline 544/1,520→541/1,515 |
| `python scripts/arc_mut_guard.py` | 通过；production 323/904、test 204/571、compatibility 14/40；Client owner 6/12、Client test 12/83；没有新增 production `mut_from_ref` |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` | 通过 |
| Client package / root workspace strict Clippy | `cargo clippy -p rocketmq-client-rust --no-deps --all-targets --all-features -- -D warnings` 与 root workspace profile 均通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；internal producer 的异步 owner 保持原 start/shutdown/send 串行范围，Pull child 使用标准共享 owner |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，guard 单测 60/60 通过 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| MQClientInstance child ArcMut 定向扫描 | production factory 文件中 PullMessageService/default producer ArcMut owner 零匹配；仅 test module 保留显式 test-only ArcMut fixture |
| `git diff --check` | 通过；仅报告工作树 CRLF 转换提示，无 whitespace error |

M11-12an 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-client-rust --all-targets --all-features` | 通过；标准 Producer root、Weak registry 与全部 Client targets 编译通过 |
| `cargo clippy -p rocketmq-client-rust --all-targets --all-features -- -D warnings` | 通过；仅有被 Rust 明确排除于 `-D warnings` 的 Windows linker stdout 提示 |
| Producer implementation focused tests | 47/47 通过；覆盖 Weak drop、resolver/detector Weak、task admission、取消恢复、config lock reload、Running 发布顺序与 latency coherent snapshot |
| `cargo test -p rocketmq-client-rust --all-features` | 退出码 0；library 1,002/1,002、全部 integration targets 与 doc tests 通过，35 项既有外部环境测试忽略 |
| reviewed baseline reduction | 删除 6 个 production identity/12 occurrence 与 8 个 test identity/12 occurrence，无 relocation；baseline 541/1,515→527/1,491 |
| `python scripts/arc_mut_guard.py` | 通过；production 317/892、test 196/559、compatibility 14/40；Client production 0/0、Client test 4/71 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| standalone Example / Tauri Rust backend / Web backend | 各自 fmt + strict Clippy 通过；Web backend `cargo build --all-targets --all-features` 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；lifecycle/task admission 由已登记 owner 管理，无新 detached runtime/task 边界 |
| architecture target/baseline 与 release guard | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，guard 单测 60/60 通过 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Client Producer ArcMut 定向扫描 | facade/implementation/registry 的 production ArcMut/WeakArcMut/constructor/`mut_from_ref` 零匹配，Client production 总量清零 |
| `git diff --check` | 通过；仅报告工作树 CRLF 转换提示，无 whitespace error |

M11-12ao 追加验证：

| 命令 | 结果 |
|---|---|
| `cargo test -p rocketmq-broker --all-features topic_route_info_manager` | 3/3 通过；覆盖标准共享表 clone、route cleanup 与后台 task lifecycle |
| `cargo test -p rocketmq-broker --all-features topic_queue_mapping_manager` | 13/13 通过；覆盖 lookup/update/decode、epoch/broker/expected-items cleanup、旧 Arc 代际稳定性与 stale cleanup 条件发布 |
| `cargo test -p rocketmq-broker lite_sharding` | 2/2 通过；DashMap publish route 读取适配保持既有 hash contract |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 550 passed、24 failed、1 ignored；失败集中在既有 lifecycle probe 与全局 Lite/consumer state tests，代表性 `get_broker_lite_info_returns_registry_aggregates` 在 clean `main@a158c49a8` 使用相同 all-features exact 命令同样失败，不能记为本切片通过，也未发现受影响定向测试失败 |
| reviewed baseline reduction | 删除 5 个 production identity/19 occurrence 与 2 个 test identity/8 occurrence，无 relocation；baseline 527/1,491→520/1,464 |
| `python scripts/arc_mut_guard.py` | 通过；production 312/873、test 194/551、compatibility 14/40；Broker production 185/549 |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace all-targets/all-features `-D warnings` profile 通过 |
| architecture target/baseline/release guards | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，dependency/release/performance guard 单测 60/60 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；表 owner 替换未增加 task/runtime 边界 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| Broker topic metadata ArcMut 定向扫描 | `TopicQueueMappingManager` production/test ArcMut 清零；`TopicRouteInfoManager` 仅保留待 Broker root 切片处理的 `ArcMut<BrokerRuntimeInner>` 回边，四张表 owner 与 `mut_from_ref` 清零 |

## M11-12ap 实现

Broker TopicConfig ownership 随 Issue #8379 完成以下边界收敛：

- `TopicConfigManager` 的 live table、读快照和 Store 注入 carrier 统一为 `Arc<TopicConfig>` 不可变整值代际，旧读者在替换后继续观察稳定旧代际，调用方不能通过共享 handle 原地修改配置。
- TopicConfig table 写入、ArcSwap snapshot 发布和 DataVersion 推进由单一 `metadata_transition` 串行；派生 flag/order 更新在 transition 内重读最新代际再 copy-update-publish，避免并发更新丢失。
- full/single/incremental NameServer 注册共用异步发送顺序锁；请求在获得锁后重新采样当前 TopicConfig/DataVersion，权限掩码只作用于 owned clone，迟到触发不会覆盖更新代际或修改 Broker 存储值。
- JSON/RocksDB 持久化在同一 metadata snapshot 中捕获 table/value 与 DataVersion，I/O 由单一 persistence lock 串行且同步 guard 不跨 I/O/`.await`；slave sync 通过 manager API 一次替换完整表和版本，mapping-only 同步不再重写 TopicConfig table。
- reviewed baseline 从 520 identities / 1,464 occurrences 降至 482 / 1,289；production 从 312/873 降至 300/783，test 从 194/551 降至 168/466，compatibility 保持 14/40。Broker 为 178/475，Store 为 122/308；净删除 12 个 production identity/90 occurrence 与 26 个 test identity/85 occurrence，无新增 identity。

## M11-12ap 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-features --all-targets` | 通过；Broker/Store TopicConfig carrier、注册、持久化和 slave replacement 在全部 target 编译通过 |
| `cargo clippy -p rocketmq-broker --all-targets --all-features -- -D warnings` | 通过；仅有 Rust 明确排除于 `-D warnings` 的 Windows linker stdout 提示 |
| TopicConfigManager focused tests | 9/9 通过；覆盖不可变代际、并发提交/持久化、master replacement、split version reservation 与 stale registration 重采样 |
| Broker permission-mask / RocksDB config focused tests | 1/1、2/2 通过；权限掩码不修改存储代际，RocksDB load/delete 保持表值与版本语义 |
| Broker TopicConfig admin endpoint focused test | 1/1 通过；GetAll/GetTopicConfig body 可解码，table 与 DataVersion 来自同一 metadata snapshot |
| Store all-target/all-feature check + strict Clippy | 通过；22 个 Store source/test/bench carrier 迁移为标准 `Arc<TopicConfig>` |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 556 passed、25 failed、1 ignored；隔离 `main@3586348b2` 为 550 passed、25 failed、1 ignored，失败测试名完全一致，本切片新增 6 个测试均通过，故不能把全套记为通过，也未新增 baseline failure |
| reviewed baseline reduction | baseline 520/1,464→482/1,289；8 个保留对象指纹位移逐项以临时 ADR-013 approval 审核且 approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；production 300/783、test 168/466、compatibility 14/40；没有新增 identity |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| architecture target/baseline/release guards | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，dependency/release/performance guard 单测 60/60 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；metadata transition、registration send ordering 与 persistence serialization 未新增 detached task/runtime 边界 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| TopicConfig ArcMut 定向扫描 / `git diff --check` | Broker/Store 的 `ArcMut<TopicConfig>`、constructor 和旧 DataVersion reference accessor 零匹配；diff check 无 whitespace error，仅报告 CRLF 转换提示 |

## M11-12aq 实现

Broker POP buffer ownership 随 Issue #8381 完成以下边界收敛：

- `PopBufferMergeService` root、checkpoint buffer 与 commit-offset queue element 统一改用标准 `Arc`；`PopCheckPointWrapper` 的 revive offset、ACK bits、stored bits 与 stored flag 继续通过原子字段发布，调用方不再获得共享可变引用。
- `add_ack`、scan/garbage collection、mock checkpoint 与 drain 路径收窄为 `&self`；扫描计数改用 `AtomicU64` 且保持原有 pre-increment log、post-increment 30 秒/分钟归零 cadence，批量 ACK scratch 由单一扫描 task 独占并跨扫描复用。
- scan 在异步持久化前克隆 `(merge key, Arc<checkpoint>)` 完整代际并释放 DashMap guard；删除使用 `Arc::ptr_eq` 条件发布，拒绝旧扫描删除并发新代际。commit-offset FIFO 不再随 buffer checkpoint 清理而整队删除，queue handle 在 `.await` 前从 DashMap guard 中克隆。
- `PopMessageProcessor` 的 construction/start/append/accessor/shutdown 与 ACK/change-invisible/admin 路径使用标准 service handle；既有 rollback test 追加重复 start，验证 running CAS 保持启动幂等且 shutdown-drain-restart 顺序不变。
- reviewed baseline 从 482 identities / 1,289 occurrences 降至 479 / 1,268；production 从 300/783 降至 298/764，test 从 168/466 降至 167/464，compatibility 保持 14/40。Broker 为 176/456；净删除 2 个 production identity/19 occurrence 与 1 个 test identity/2 occurrence，无新增 identity。

## M11-12aq 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-targets --all-features` / Broker strict Clippy | 通过；标准 Arc service/checkpoint carrier、共享 receiver、scan lifecycle 与全部 Broker targets/features 编译及 lint 通过 |
| POP buffer focused tests / rollback lifecycle test | 12/12、1/1 通过；覆盖标准 Arc 跨线程原子状态可见性、key/offset wrapper、RocksDB record 映射、重复 start 与 graceful drain/restart |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 558 passed、24 failed、1 ignored；失败名是上一 main 已记录 25 项失败集合的严格子集，本切片 focused tests 全通过，故不能把全套记为通过，也未新增 baseline failure |
| reviewed baseline reduction | baseline 482/1,289→479/1,268；1 个保留的 BrokerRuntimeInner 参数 occurrence 因相邻 return carrier 变更发生指纹位移，以临时 ADR-013 approval 审核且 approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；production 298/764、test 167/464、compatibility 14/40，Broker production 176/456；没有新增 identity |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| architecture target/baseline/release guards | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，dependency/release/performance guard 单测 60/60 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；scan task 仍由 TaskGroup 所有，running CAS、shutdown/drain/restart 未新增 detached task/runtime 边界 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| POP buffer ArcMut 定向扫描 / `git diff --check` | service/checkpoint carrier、constructor 与 service-self `mut_from_ref` 零匹配；保留 BrokerRuntimeInner root boundary 与 3 个 escape bridge 调用供后续切片，diff check 无 whitespace error |

## M11-12ar 实现

Broker POP lifecycle ownership 随 Issue #8383 完成以下边界收敛：

- `PopMessageProcessor` 与 `NotificationProcessor` root、两者拥有的 `PopLongPollingService` 统一改用标准 `Arc`；构造器通过 `Arc::new_cyclic` 一次安装标准 `Weak` processor 回边，删除运行期 setter 和 processor/service 强引用环。
- crate-private shared wake-up trait 接收 owned request 并经 processor `&self` handler 分发；Broker processor enum、ACK/change-invisible 依赖与 BrokerRuntime fields/accessors 使用标准 `Arc`，不引入整 processor 异步 mutex 或 `mut_from_ref`。
- 长轮询 scan task 只捕获 service `Weak`，拆除 service/TaskGroup/task 强引用环；每轮 upgrade 覆盖单次同步扫描，owner 已释放时任务自然退出。cleanup 时间改用 `AtomicU64`，清理与 shutdown 使用共享 receiver。
- service 与 processor 分别用异步 lifecycle gate 串行完整 start/shutdown/restart；TaskGroup 在 scan spawn 前发布，短 parking_lot slot guard 在任何 `.await` 前释放，请求/wake-up 路径不获取 lifecycle gate。
- 新增标准 Arc Send+Sync、POP/Notification Weak 回边、活动 scan 不保活 owner、重复 start/停止/重启覆盖；既有 SQL request、rollback drain/restart 与 Broker 请求码分发回归保持通过。
- reviewed baseline 从 479 identities / 1,268 occurrences 降至 476 / 1,241；production 从 298/764 降至 296/738，test 从 167/464 降至 166/463，compatibility 保持 14/40。Broker 为 174/430；净删除 2 个 production identity/26 occurrence 与 1 个 test identity/1 occurrence，无新增 identity。

## M11-12ar 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-targets --all-features` / Broker strict Clippy | 通过；标准 Arc/Weak owner、共享 request handler、异步 lifecycle gate 与全部 Broker targets/features 编译及 lint 通过 |
| POP processor / Notification / rollback / Broker dispatch focused tests | 22/22、1/1、1/1、1/1 通过；覆盖弱回边、活动 scan owner drop、重复启动/停止/重启、SQL request、graceful rollback 与请求码注册 |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 562 passed、24 failed、1 ignored；相比 `main@f5d8efeec` 已记录的 558 passed、24 failed、1 ignored，新增 4 个测试全部通过且失败名单完全一致，故不能把全套记为通过，也未新增 baseline failure |
| reviewed baseline reduction | baseline 479/1,268→476/1,241；14 个保留 occurrence 因相邻 carrier/签名变化发生一对一指纹位移，以临时 ADR-013 approval 逐项审核且 approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；production 296/738、test 166/463、compatibility 14/40，Broker production 174/430；没有新增 identity |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| architecture target/baseline/release guards | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates，dependency/release/performance guard 单测 60/60 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；scan task 仍由 TaskGroup 所有，Weak capture 与串行 lifecycle transition 未新增 detached task/runtime 边界 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| POP lifecycle ArcMut 定向扫描 / `git diff --check` | POP/Notification processor root、long-poll service carrier/constructor/back-reference 与对应 `mut_from_ref` 零匹配；保留 `BrokerRuntimeInner`/`GetMessageResult` 既有边界，diff check 无 whitespace error |

## M11-12as 实现

Broker POP Lite lifecycle ownership 随 Issue #8385 完成以下边界收敛：

- `PopLiteMessageProcessor`、`PopLiteLongPollingService`、Broker processor enum 与 BrokerRuntime field/accessor 统一改用标准 `Arc`；processor 通过 `Arc::new_cyclic` 向 service 一次安装标准 `Weak` 回边，删除运行期 setter 与 processor/service 强引用环。
- crate-private shared wake-up trait 接收 owned request 并经 processor `&self` handler 分发；remoting `RequestProcessor` 仅保留为兼容 adapter，不为标准 Arc 引入整 processor mutex 或共享 `&mut` 别名。
- scan task 只捕获 service `Weak` 和本轮 receiver；每次成功 start 创建新 channel，spawn 成功后向 `LiteEventDispatcher` 发布 sender，shutdown/restart 不再复用已消费或已关闭的 receiver，owner 释放时 scan 自然退出。
- processor 与 service 分别使用异步 lifecycle gate 串行 start/shutdown/restart；TaskGroup 在 spawn 前发布、失败时回滚，短 parking_lot guard 在任何 `.await` 前释放，停止状态拒绝新增挂起以避免 drain 后重新入队。
- 新增标准 Arc Send+Sync、Weak 回边、重复 start/停止/重启与活动 scan 不保活 owner 覆盖；保留的两个目标文件仍有 4 个 `BrokerRuntimeInner` 与 2 个 MessageStore `ArcMut` type reference 及 2 个 import occurrence，留待 Broker root/Store 边界后续切片处理。
- reviewed baseline 从 476 identities / 1,241 occurrences 降至 473 / 1,227；production 从 296/738 降至 294/725，test 从 166/463 降至 165/462，compatibility 保持 14/40。Broker production 为 172/417、test 为 65/81；净删除 2 个 production identity/13 occurrence 与 1 个 test identity/1 occurrence，无新增 identity。

## M11-12as 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-targets --all-features` / Broker strict Clippy | 通过；标准 Arc/Weak owner、共享 request handler、重启 channel 与双层异步 lifecycle gate 在全部 Broker targets/features 编译及 lint 通过 |
| POP Lite lifecycle focused tests | 3/3 通过；覆盖 Send+Sync/Weak 回边、重复 start/停止/重启、活动 scan 不保活 owner |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过；隔离 main 对照污染增量缓存后曾触发 nightly rustc ICE，按编译器建议定向 clean 后重跑通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 564 passed、25 failed、1 ignored；其中 24 项与上一 main 已登记失败集合一致，额外 lifecycle probe 已在隔离 `main@2c301e71a` 以同一聚焦测试命令复现；新增 3 个测试全部通过，故不能把全套记为通过，也未新增 baseline failure |
| reviewed baseline reduction | baseline 476/1,241→473/1,227；5 个保留 occurrence 因相邻 carrier/签名/测试 import 变化发生一对一指纹位移，以临时 ADR-013 approval 逐项审核且 approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；production 294/725、test 165/462、compatibility 14/40，Broker production 172/417；没有新增 identity |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| architecture target/baseline/release/performance guards | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates、8 profiles/11 variants/50 metric contracts，dependency/release/performance guard 单测 60/60 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；scan task 仍由 TaskGroup 所有，Weak capture、fresh receiver 与串行 lifecycle transition 未新增 detached task/runtime 边界 |
| `./scripts/check-agents-routing.ps1` | 通过；4 个 standalone Cargo、3 个 Node project、8 条 route |
| POP Lite ArcMut 定向扫描 / `git diff --check` | processor/service root、Broker carrier、processor back-reference、service-self `mut_from_ref` 与测试 glob import 零匹配；保留 BrokerRuntimeInner/MessageStore 既有边界，diff check 无 whitespace error |

下一子切片 M11-12at 处理 `PullMessageProcessor`/`PullRequestHoldService` lifecycle owner；75/82 总进度不变，
Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

## M11-12at 实现

Broker Pull lifecycle ownership 随 Issue #8387 完成以下边界收敛：

- `PullMessageProcessor`、`DefaultPullMessageResultHandler`、`PullRequestHoldService`、Broker processor enum/runtime carrier 与未启用的 V2 migration example 统一改用标准 `Arc`；remoting `RequestProcessor` 只保留无可变 capability 的兼容 adapter，实际 Pull/LitePull 分发使用共享 `&self` 入口。
- hold service 只保留 processor 标准 `Weak` 回边，并通过 crate-private 窄 capability 读取 long-polling 配置、Store max offset 与提交 wake-up；service 不再强持有 BrokerRuntimeInner，processor/service/runtime 强引用环已拆除。
- scan task 只捕获 service `Weak`、schedule signal 和 cancellation token；每轮等待前释放升级得到的 service owner。异步 lifecycle gate 串行 start/shutdown/restart，TaskGroup 在 spawn 前发布且失败回滚，shutdown 停止准入、等待 scan 并清空挂起请求与 deadline。
- suspend 在 table write guard 内检查 running 与 processor 存活，停止或 processor 已释放时返回原 `PullNotFound` response，避免请求进入无 scanner 队列；deadline rebuild 在 table read guard 内完成 atomic publish，避免覆盖并发新 deadline。
- master-online wake-up 先移出 owned requests、锁外提交；`NotifyMinBrokerChangeIdHandler` 更新 snapshot 后立即释放 Tokio RwLock write guard，不再写锁内重入 read lock 或跨后续 `.await` 持锁。
- reviewed baseline 从 473 identities / 1,227 occurrences 降至 465 / 1,204；production 从 294/725 降至 289/706，test 从 165/462 降至 162/458，compatibility 保持 14/40。Broker production 为 167/398、test 为 62/77；净删除 5 个 production identity/19 occurrence 与 3 个 test identity/4 occurrence，无新增 identity。

## M11-12at 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-targets --all-features` | 通过；标准 Arc/Weak owner、共享 request handler、异步 lifecycle、stopped admission 与全部 Broker targets/features 编译通过 |
| Pull hold-service / processor / Broker dispatch focused tests | 5/5、14/14、1/1 通过；覆盖 weak back-reference、活动 scan owner drop、start/shutdown/restart、spawn failure rollback、TaskGroup wake-up worker、Pull 处理逻辑和 request-code 注册 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 567 passed、25 failed、1 ignored；25 个失败与上一 main 已记录失败名单完全一致，本切片新增净 3 个 lifecycle 测试均通过，故不能把全套记为通过，也未新增 baseline failure |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| reviewed baseline reduction | baseline 473/1,227→465/1,204；3 个保留 occurrence 因相邻 carrier 变化发生一对一指纹位移，以临时 ADR-013 approval 逐项审核且 approval 不提交 |
| `python scripts/arc_mut_guard.py` | 通过；production 289/706、test 162/458、compatibility 14/40，Broker production 167/398；没有新增 identity |
| `python -m unittest scripts.tests.test_arc_mut_guard -v` / `python scripts/arc_mut_guard.py --fixtures` | 67/67 单测、24/24 fixtures 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| architecture target/baseline/release/performance guards | 通过；35/35 target edges、3/3 test edges、32/32 release topology、10/10 R0 crates、8 profiles/11 variants/50 metric contracts，dependency/release/performance guard 单测 60/60 通过 |
| `./scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过；scan task 由 TaskGroup 所有，Weak capture 与串行 lifecycle transition 未新增 detached task/runtime 边界 |
| `./scripts/check-agents-routing.ps1` / `git diff --check` | routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12au 处理 `ConsumerOffsetManager` immutable/serialized ownership；75/82 总进度不变，Broker/Store、
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12au 实现

Broker ConsumerOffsetManager ownership 随 Issue #8389 完成以下边界收敛：

- `ConsumerOffsetManager` 保持由 BrokerRuntimeInner 按值独占，不新增 root `Arc`；删除 manager/wrapper 无调用的 Clone、完整可写 offset-table lock escape、无用 mutable accessor 与 setter。Lite lag/diagnostics 只读取 owned snapshot 或长度。
- `DataVersion` 改由 `ArcSwap` 发布完整不可变代际；单一短 transition 串行 offset/reset/pull 写入、commit 计数和版本发布，旧 reader 在更新后继续持有旧 snapshot。`ArcMut<MessageStore>` 初始化 carrier 明确延期到 Store/root 切片。
- `commit_offset` 使用 `fetch_add` 返回值精确判断 `consumer_offset_update_version_step`，并发 writer 不再经第二次 load 重复或漏过阈值；非正 step 不再触发取模除零 panic。
- Broker pre-online/slave 同步通过单一 `merge_offsets_from_peer` 安装表与版本，不再分别取得可写 DataVersion/table owner；read-only admin encode 改用共享 accessor。
- JSON serialize 在 transition 内构造一致视图，decode 即使主 offset 表为空也恢复 reset/pull/version；RocksDB 先完整读取和解析全部记录，成功后一次发布，解析失败不再留下新版本或部分新表。RocksDB persist 在锁内克隆表与版本、锁外编码和 I/O。
- reviewed baseline 从 465 identities / 1,204 occurrences 降至 461 / 1,194；production 从 289/706 降至 287/699，test 从 162/458 降至 160/455，compatibility 保持 14/40。Broker production 为 165/391、test 为 60/74；净删除 2 个 production identity/7 occurrence 与 2 个 test identity/3 occurrence，无新增 identity或 fingerprint relocation。

## M11-12au 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-targets --all-features` | 通过；ArcSwap generation、transition、narrow peer/table API 与全部 Broker targets/features 编译通过 |
| ConsumerOffset focused default/all-feature tests | 13/13、15/15 通过；覆盖旧 snapshot 隔离、16 writer 精确阈值、零步长、peer merge、空主表 JSON reset/pull round-trip 与 RocksDB restart/delete |
| processor/export/file→RocksDB migration focused tests | 1/1、1/1、2/2 通过；wire handler round-trip、JSON export、single/separate RocksDB metadata migration/recovery 保持通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 572 passed、25 failed、1 ignored；相对上一 main 的 567 passed、25 failed、1 ignored，新增 5 个测试全部通过且失败名单完全一致，故不能把全套记为通过，也未新增 baseline failure |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 465/1,204→461/1,194，无 relocation/临时 approval；guard 通过，production 287/699、test 160/455、compatibility 14/40，Broker production 165/391 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12av 处理 Broker `ScheduleMessageService` 内部 offset/delay/DataVersion/status ownership；75/82 总进度
不变，Broker root/其他 processor/transaction、Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate
仍保持开放。

## M11-12av 实现

Broker ScheduleMessageService internal-state ownership 随 Issue #8391 完成以下边界收敛：

- delay level table 与 max level 合并为一个 `ArcSwap<DelayLevelConfig>` 不可变代际；纯解析函数先验证全部 level、正数和乘法溢出，再一次发布。非法非 ASCII unit 不再触发字符串边界 panic，缩短配置时旧 pending/resend level 同步移除。
- offset table、version cadence 与 `DataVersion` 由一个短 `ScheduleOffsetState` transition 串行；普通推进拒绝相同或回退 offset，第 step 次精确发布新版本，step=0 不取模 panic。snapshot 在同一锁域复制 offset/version，安装 peer snapshot 使用整表替换并重置 cadence。
- Broker pre-online/slave 在写入远端 JSON 前先完整 decode，并把已解析 wrapper 直接交给 service 安装；`load_when_sync_delay_offset` 不再调用空实现而遗漏 live offset/version 同步。
- `ProcessStatus` 改为 Acquire/Release `AtomicU8`；pending queue 与 DashMap guard 不再跨 message-store 或 resend `.await`。显式 per-level resend-in-progress 状态保证重复 handler 不会越过失败队首，put 已发生后即使并发达到软容量也仍保留结果跟踪。
- shutdown API 收窄为共享 receiver，删除 3 个零调用的 BrokerRuntime mutable accessor/setter 与测试别名。Schedule service/root、delivery task 与 `PutResultProcess` 的 BrokerRuntime `ArcMut` carrier、两处跨 await EscapeBridge mutable root、task generation、shutdown ordering 和 blocking persistence 明确留给 M11-12aw，不宣称完整 schedule ownership 已结束。
- reviewed baseline 从 461 identities / 1,194 occurrences 降至 459 / 1,173；production identity 保持 287、occurrence 从 699 降至 680，test 从 160/455 降至 158/453，compatibility 保持 14/40。Broker production 为 165/372、test 为 58/72；净删除 19 个 production occurrence 与 2 个 test identity/2 occurrence。3 个保留 occurrence 仅因相邻 owner/测试变化发生一对一 fingerprint relocation，经临时 ADR-013 approval 审核且 approval 不提交。

## M11-12av 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-targets --all-features` / Broker strict Clippy | 通过；immutable config generation、serialized offset/version state、atomic status 与全部 Broker targets/features 编译和 lint 通过 |
| ScheduleMessageService focused default/all-feature tests | 18/18、18/18 通过；新增 7 个测试覆盖精确阈值、step=0、旧 generation、16 writer、snapshot replace/cadence reset、完整配置发布、非 ASCII/溢出输入和 status transition |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 580 passed、24 failed、1 ignored；24 个失败均属于上一 main 已登记的 25 个失败集合，本切片新增 7 个测试全部通过且未新增 baseline failure，故不能把全套记为通过 |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 461/1,194→459/1,173；3 个保留 occurrence 经临时 ADR-013 一对一 relocation 审核，approval 不提交；guard 通过，production 287/680、test 158/453、compatibility 14/40，Broker production 165/372 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12aw 处理 Schedule root capability、task generation、shutdown ordering 与 blocking persistence ownership；
75/82 总进度不变，Broker 其他 processor/transaction、Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照
Gate 仍保持开放。

## M11-12aw 实现

Broker Schedule root/lifecycle ownership 随 Issue #8393 完成以下边界收敛：

- `ScheduleMessageService` root 从 `ArcMut` 迁移到标准 `Arc`；Broker outer 持有唯一 EscapeBridge strong owner，BrokerRuntimeInner 只保留标准 `Weak` 回边，拆除 inner→bridge→inner 强引用环。Schedule、POP、ACK 和 transaction 调用链统一使用共享 EscapeBridge capability，不再通过 `mut_from_ref`/mutable accessor 跨 `.await`。
- 每次成功 start 分配递增 generation 和独立 `TaskGroupChildLease`；delivery、put-result 与周期持久化任务只捕获 service `Weak`、generation context 和 cancellation token。activation gate 保证任务安装失败可事务回滚，重调度只能回到同一 generation，所有 sleep/backoff 均可取消，stop 后动态 child 被 drain 并从父组剪枝，restart 使用全新 generation。
- 单一异步 lifecycle gate 串行 start/stop/shutdown，单一 persistence gate 串行周期、最终和 peer 写入。停止先清除 active generation、取消并 drain 旧组，再执行唯一最终 offset 持久化；final persist 失败保持可重试状态。生产 load、周期/最终/peer 文件 I/O 全部经注入的 `ServiceContext::blocking` 所有；未显式注入 context 的公开 Builder compatibility 路径复用 `ScheduledTaskManager` 已审计 legacy root，并只在其下创建 Schedule TaskGroup/BlockingExecutor child，不新增 Broker current-runtime adapter。
- peer delay-offset 同步先持久化完整 JSON，再安装内存 snapshot；写盘失败时 offset/version 保持不变。Broker shutdown 在 message-store shutdown 前完成 schedule cancel/drain/final persist 并移除 runtime slot；Controller 降级在 Store 仍为 Master 时先停止 schedule，错误向角色切换传播，成功后才发布 schedule 状态位和 Store Slave role；提升保持先切 Store Master 再启动 schedule。
- `PutResultProcess` 不再强持有 BrokerRuntimeInner，只保存 EscapeBridge `Weak` 与不可变 resend limit；重试退避可取消，取消时 process 放回队首。启动 load 不再重复执行，Schedule diagnostics 同时统计 generation 父组与 scheduled child driver。
- reviewed baseline 从 459 identities / 1,173 occurrences 降至 453 / 1,146；production 从 287/680 降至 282/654，test 从 158/453 降至 157/452，compatibility 保持 14/40。Broker production 从 165/372 降至 160/346、test 从 58/72 降至 57/71；净删除 5 个 production identity/26 occurrence 与 1 个 test identity/1 occurrence。1 个保留 import occurrence 因相邻 import 删除发生一对一 fingerprint relocation，经临时 ADR-013 approval 审核且 approval 不提交。

## M11-12aw 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-features` / Broker strict Clippy | 通过；标准 root/Weak 回边、generation lifecycle、BlockingExecutor I/O 和 role/shutdown ordering 在 all-features 下编译和 lint 通过 |
| Schedule/EscapeBridge/lifecycle focused tests | Schedule 21/21、EscapeBridge 5/5、持久化 lifecycle probe 1/1、角色状态失败重试 1/1 通过；覆盖 fresh generation/child prune/restart、legacy Builder owned context、peer disk failure 不发布内存、最终持久化失败不发布角色状态与修复后重试 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 583 passed、25 failed、1 ignored；25 个失败与 main 已登记失败名单一致，本切片新增 4 个测试全部通过且未新增 baseline failure，故不能把全套记为通过 |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 459/1,173→453/1,146；1 个保留 occurrence 经临时 ADR-013 一对一 relocation 审核，approval 不提交；guard 通过，production 282/654、test 157/452、compatibility 14/40，Broker production 160/346 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / targeted scan / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；Schedule root ArcMut、EscapeBridge mutable accessor/path 与 Schedule 文件 ArcMut 均零匹配；diff check 无 whitespace error |

下一子切片 M11-12ax 继续处理 Broker 其他 processor/transaction owner；75/82 总进度不变，Broker root 其余债务、
Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12ax 实现

Broker transaction service ownership 随 Issue #8395 完成以下边界收敛：

- `DefaultTransactionalMessageService` root、send/reply/end-transaction processor capability 从 `ArcMut` 迁移为标准 `Arc`；`TransactionalOpBatchService` 对 service 的后台回边从 `WeakArcMut` 迁移为标准 `Weak`，worker 无法延长 service 生命周期。
- transactional service prepare/delete/commit/rollback/check API 改为共享引用；原先依赖独占引用的 `TransactionalMessageBridge` 操作由 service 内单一 Tokio mutex 显式串行，commit/rollback 改为 async 以避免同步路径逃逸共享可变引用。
- transaction check service 直接注入不可变 BrokerConfig、标准 Arc service 与 listener，`on_wait_end` 不再从 BrokerRuntimeInner 经 `mut_from_ref` 取得 mutable service；停止信号在 wait 返回后再次检查，不会在 shutdown 后启动新一轮 check。listener 的无状态 Broker2Client 从 `ArcMut` 迁移到标准 Arc。
- Broker shutdown 先 drain check service 与 listener-owned tasks，再关闭 transaction service/op-batch worker。batch sender 在 delete-context 锁内只生成 ready queue snapshot，释放锁后再获取 op message 与执行写入；offer 失败回退也先释放 context 锁，删除同一 Tokio mutex 的嵌套获取风险。
- reviewed baseline 从 453 identities / 1,146 occurrences 降至 444 / 1,125；production 从 282/654 降至 274/634，test 从 157/452 降至 156/451，compatibility 保持 14/40。Broker production 从 160/346 降至 152/326、test 从 57/71 降至 56/70；transaction production 子树从 13/19 降至 5/8。净删除 8 个 production identity/20 occurrence 与 1 个 test identity/1 occurrence；6 个 retained occurrence 因相邻 service root 删除及 field 重排发生一对一 fingerprint relocation，经临时 ADR-013 approval 审核且 approval 不提交。

## M11-12ax 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-features` / Broker strict Clippy | 通过；标准 service Arc/Weak、共享 transaction API、显式 bridge serialization 与 injected check capability 在 all-features 下编译和 lint 通过 |
| `cargo test -p rocketmq-broker --all-features transaction --lib` | 24/24 通过；包含 ownership source contract、transaction decision helpers、listener conversion 与 end-transaction message conversion |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 584 passed、25 failed、1 ignored；25 个失败与 main 已登记失败名单一致，本切片新增 ownership test 通过且未新增 baseline failure，故不能把全套记为通过 |
| RocksDB specialized gates | Store/Broker strict Clippy 通过；foundation 82/82、semantics 9/9、Broker `rocksdb` 20/20、`pop_consumer` 4/4 通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 453/1,146→444/1,125；6 个 retained occurrence 经临时 ADR-013 一对一 relocation 审核，approval 不提交；guard 通过，production 274/634、test 156/451、compatibility 14/40，Broker production 152/326 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12ay 继续处理 Broker 其他 processor 及 transaction bridge/listener carrier；75/82 总进度不变，
Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12ay 实现

Broker transaction processor root ownership 随 Issue #8398 完成以下边界收敛：

- send/reply/end-transaction processor 在 `BrokerProcessorType` 与 Broker startup 的 root 从 `ArcMut` 迁移为标准 `Arc`；每个 processor 提供共享入口，按请求复制只含共享 capability 的轻量句柄后执行既有可变 receiver 路径，不复制业务状态、不引入 processor 级全局 mutex，也不串行并发请求。
- `BrokerRequestProcessor` 的 request table 与 default processor 从 `ArcMut` 迁移为标准 `Arc`；启动期 `register_processor` 使用 `Arc::make_mut` copy-on-write，server clone 后只共享不可变路由表。
- source ownership contract 固定三个 transaction-related variant、对应 startup constructor 与 registry carrier 必须使用标准 `Arc`，防止后续回退到共享可变 root。
- reviewed baseline 从 444 identities / 1,125 occurrences 降至 443 / 1,115；production 从 274/634 降至 273/624，test 保持 156/451，compatibility 保持 14/40。Broker production 从 152/326 降至 151/316；净删除 1 个 production identity/10 occurrence。2 个 retained processor variant 因相邻 transaction root 删除发生一对一 fingerprint relocation，经临时 ADR-013 approval 审核且 approval 不提交。

## M11-12ay 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-features` / Broker strict Clippy | 通过；标准 processor root、per-request capability clone 与 immutable registry 在 all-features/all-targets 下编译和 lint 通过 |
| transaction processor ownership / transaction focused tests | ownership source contract 1/1、transaction 25/25 通过；覆盖标准 root/constructor/registry 与既有 transaction decision/listener/conversion 行为 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 585 passed、25 failed、1 ignored；25 个失败与 main 已登记失败名单一致，本切片新增 ownership test 通过且未新增 baseline failure，故不能把全套记为通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 444/1,125→443/1,115；2 个 retained occurrence 经临时 ADR-013 一对一 relocation 审核，approval 不提交；guard 通过，production 273/624、test 156/451、compatibility 14/40，Broker production 151/316 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12az 继续处理 transaction bridge/listener capability 与其他 Broker owner；75/82 总进度不变，
Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12az 实现

Broker core request processor roots 随 Issue #8400 完成以下边界收敛：

- peek、polling-info、recall、query-message、client-manage、consumer-manage 与 query-assignment processor 在 `BrokerProcessorType` 与 Broker startup 的 root 从 `ArcMut` 迁移为标准 `Arc`；query-assignment 的 Broker runtime slot 与只读 accessor 同步改为标准 `Arc`，删除无调用方的 mutable accessor。
- 除 Peek 外，共享入口按请求复制只含共享 capability 的轻量 processor 句柄后复用既有 `RequestProcessor` 路径，不复制业务表、不引入 processor 级全局 mutex，也不串行并发请求；Peek 的内部处理直接收窄为 `&self`，保持同一 root 的唯一 `AtomicU32` 序列状态。
- `MessageRequestModeManager` 的 clone 共享既有 `Arc<Mutex<MessageRequestModeMap>>` 与不可变配置，QueryAssignment 的 per-request 句柄不会复制请求模式表或负载策略状态。
- source ownership contract 固定七个 variant、startup constructor 与 query-assignment runtime carrier 必须使用标准 `Arc`，防止后续回退到共享可变 root。
- reviewed baseline 从 443 identities / 1,115 occurrences 降至 443 / 1,096；production identity 保持 273、occurrence 从 624 降至 605，test 保持 156/451，compatibility 保持 14/40。Broker production identity 保持 151、occurrence 从 316 降至 297；净删除 19 个 production occurrence。1 个 retained LiteManager variant 因相邻 QueryAssignment root 删除发生一对一 fingerprint relocation，经临时 ADR-013 approval 审核且 approval 不提交。

## M11-12az 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-features` / Broker strict Clippy | 通过；七类标准 processor root、共享入口与 query-assignment runtime carrier 在 all-features/all-targets 下编译和 lint 通过 |
| core processor ownership / phase 3、phase 4 dispatch / query-assignment focused tests | ownership 1/1、两组 dispatch 各 1/1、query-assignment 10/10 通过；覆盖标准 root/constructor/runtime slot、既有 request-code 路由与负载分配行为 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 586 passed、25 failed、1 ignored；25 个失败与 main 已登记失败名单一致，本切片新增 ownership test 通过且未新增 baseline failure，故不能把全套记为通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 443/1,115→443/1,096；1 个 retained occurrence 经临时 ADR-013 一对一 relocation 审核，approval 不提交；guard 通过，production 273/605、test 156/451、compatibility 14/40，Broker production 151/297 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12ba 配对拆分 transaction bridge/listener 与 TopicConfigManager runtime capability carrier，并继续处理
Broker admin/其他 processor；75/82 总进度不变，Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate
仍保持开放。

## M11-12ba 实现

Broker auth admin handler ownership 随 Issue #8402 完成以下边界收敛：

- create/update/delete/get/list user 与 ACL，以及 global-white update 共 11 个 auth admin handler 删除从未读取的 `ArcMut<BrokerRuntimeInner<MS>>` 字段和构造参数；handler 只保留实际使用的标准 `Arc<AuthAdminService>` capability。
- handler 类型删除不再表达真实依赖的 `MS: MessageStore` 泛型，`AdminBrokerProcessor` startup wiring 不再向这些 narrow control-plane handler 复制完整 Broker runtime owner。
- auth ACL/user round-trip、malformed body、super-user protection、empty list、policy-type delete 与 global-white snapshot 测试继续使用同一 `AuthAdminService` 状态，行为与 wire response 不变。
- source ownership contract 固定 11 个 handler 文件不得重新引入 `BrokerRuntimeInner` 或 `ArcMut`，防止构造兼容参数再次变为无意义强持有。
- reviewed baseline 从 443 identities / 1,096 occurrences 降至 429 / 1,071；production 从 273/605 降至 259/580，test 保持 156/451，compatibility 保持 14/40。Broker production 从 151/297 降至 137/272；净删除 14 个 production identity/25 occurrence。1 个 test-module import 因新增 source contract 发生同 item 一对一 fingerprint relocation，经临时 ADR-013 approval 审核且 approval 不提交。

## M11-12ba 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-features` / Broker strict Clippy | 通过；无 runtime owner 的 auth admin handler 与 Admin processor wiring 在 all-features/all-targets 下编译和 lint 通过 |
| auth admin ownership / ACL / global-white / phase-7 focused tests | ownership 1/1、ACL/user 6/6、global-white 2/2、phase-7 lifecycle 与 dispatch 各 1/1 通过；覆盖 capability-only 构造与既有 auth admin 行为 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 587 passed、25 failed、1 ignored；25 个失败与 main 已登记失败名单一致，本切片新增 ownership test 通过且未新增 baseline failure，故不能把全套记为通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 443/1,096→429/1,071；1 个 test occurrence 经临时 ADR-013 一对一 relocation 审核，approval 不提交；guard 通过，production 259/580、test 156/451、compatibility 14/40，Broker production 137/272 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

## M11-12bb 实现

Broker registration carrier ownership 随 Issue #8404 完成以下边界收敛：

- `BrokerOuterAPI::register_broker_all` 删除从未读取的 `MS: MessageStore` 泛型和 `ArcMut<BrokerRuntimeInner<MS>>` 参数；三个 Broker 注册调用点不再复制完整 runtime owner 到 NameServer client boundary。
- full registration 的 `TopicQueueMappingInfo` 采样直接生成 owned `HashMap`；`TopicConfigManager::build_serialize_wrapper_with_topic_queue_map` 直接接收并移动该 wire-compatible map，不再构造 `DashMap<CheetahString, ArcMut<TopicQueueMappingInfo>>` 后逐项重新克隆。
- source contract 固定注册 API 签名不得重新引入 `BrokerRuntimeInner`、`ArcMut` 或 `MessageStore`；owned mapping 回归测试证明 key/value、epoch、queue count 与 broker identity 进入 wrapper 后保持不变。
- reviewed baseline 从 429 identities / 1,071 occurrences 降至 426 / 1,066；production 从 259/580 降至 257/576，test 从 156/451 降至 155/450，compatibility 保持 14/40。Broker production 从 137/272 降至 135/268；净删除 2 个 production identity/4 occurrence 与 1 个 test identity/1 occurrence，无 fingerprint relocation，因此无需 ADR-013 临时 approval。

## M11-12bb 验证

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-broker --all-features` / Broker strict Clippy | 通过；注册 API、三个调用点和 owned mapping wrapper 在 all-features/all-targets 下编译和 lint 通过 |
| owned registration focused tests | 新增 source/wrapper 2/2、TopicConfigManager 10/10、registration 4/4、need-register 2/2、wire request 1/1 通过；覆盖 API ownership、版本重采样、权限投影与 wire body |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 589 passed、25 failed、1 ignored；25 个失败与 main 已登记失败名单一致，较 M11-12ba 增加的 2 个通过测试正是本切片回归测试，故未新增 baseline failure，也不能把全套记为通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 429/1,071→426/1,066；无 relocation；guard 通过，production 257/576、test 155/450、compatibility 14/40，Broker production 135/268 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release/performance profiles 全绿 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12bc1 将 TopicConfigManager 收敛为无 BrokerRuntime back-reference 的非泛型标准 Arc metadata owner；
随后由 M11-12bc2 建立独立 coordinator 并管理 BlockingExecutor persistence、registration ordering 与
drain-before-unregister。75/82 总进度不变，Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc1 实现

TopicConfigManager runtime ownership cycle 随 Issue #8406 完成以下前置边界收敛：

- `TopicConfigManager<MS>` 改为非泛型 `TopicConfigManager`，Broker runtime slot 使用标准 `Arc<TopicConfigManager>`；manager 删除 `ArcMut<BrokerRuntimeInner<MS>>` back-reference、runtime accessor 及 mutable/unchecked accessor。
- manager 只保留 topic metadata、持久化路径和实时 Rocks WAL policy；topic mutation 显式接收 message-store state-machine generation，auto-create、async persist 与 single-register policy 在 Broker workflow 中实时采样，避免保留完整 runtime/config owner。
- topic create 异步任务直接捕获 manager `Arc`，不再从 runtime slot 重新取 manager；`TopicConfigAsyncPersistGuard` 在 task 正常完成、错误、panic、abort 或 spawn 失败时统一释放 pending persist 计数。
- reviewed baseline 从 426 identities / 1,066 occurrences 降至 424 / 1,061；production 从 257/576 降至 255/571，test 保持 155/450，compatibility 保持 14/40。Broker production 从 135/268 降至 133/263；净删除 2 个 production identity/5 occurrence，无 fingerprint relocation，因此无需 ADR-013 临时 approval。

## M11-12bc1 验证

| 命令 | 结果 |
|---|---|
| Broker check / strict Clippy | `cargo check -p rocketmq-broker --lib`、`--all-targets` 与 `cargo clippy -p rocketmq-broker --all-targets --all-features -- -D warnings` 通过 |
| TopicConfigManager focused tests | default feature 12/12、`rocksdb_store` 14/14 通过；覆盖无 runtime back-reference source contract、pending guard、并发代际及 Rocks restart/delete |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 591 passed、25 failed、1 ignored；25 个失败与 main 已登记名单一致，本切片新增 2 个通过测试且未新增 baseline failure，因此全套仍如实记为基线复现而非通过 |
| reviewed baseline reduction / `python scripts/arc_mut_guard.py` | baseline 426/1,066→424/1,061；无 relocation；guard 通过，production 255/571、test 155/450、compatibility 14/40，Broker production 133/263 |
| ArcMut / architecture guard tests | ArcMut 67/67、fixtures 24/24、architecture dependency/release/performance 60/60 通过；target/baseline/release 与 8-profile performance validation 全绿 |
| RocksDB specialized matrix | Store/Broker strict Clippy、Store foundation 82/82、semantics 9/9、Broker Rocks 20/20、POP consumer 4/4 通过 |
| `cargo fmt --all -- --check` / root workspace strict Clippy | 通过；root workspace 32 个 package 的 all-targets/all-features `-D warnings` profile 通过 |
| runtime audit / AGENTS routing / `git diff --check` | runtime boundary 与 routing 通过；4 个 standalone Cargo、3 个 Node project、8 条 route；diff check 无 whitespace error |

下一子切片 M11-12bc2 建立独立 persistence/registration coordinator，将同步文件/Rocks I/O 交给 `BlockingExecutor`，
关闭 task admission 并 drain 后再 unregister，且统一共享 Rocks backend 的 aggregate close；随后继续 transaction
bridge/listener capability。75/82 总进度不变。

## M11-12bc2 实现

Topic persistence/registration coordinator 随 Issue #8408 完成以下生命周期收敛：

- 新增非泛型 `TopicConfigCoordinator`，Broker 生命周期内只租用一个 dynamic child generation 和一个有界 FIFO worker；异步 Topic create 只等待有界入队，不再为每个 Topic 创建 TaskGroup child，也不再创建 current-runtime root fallback。
- Topic manager 的 update/list/delete/order/unit mutator 只发布内存 metadata 代际；Broker auto-create、Topic admin、从节点同步、POP retry、full/single/increment registration 与 Topic JSON export 均经 coordinator。文件和 RocksDB load/persist/export 只通过已建立的 `BlockingExecutor` 执行。
- coordinator 关闭 admission 后，将 Finalize 排在所有已接纳命令之后；最终写盘循环到持久版本等于当前 metadata 版本，worker、registration 与 blocking task 全部静默后，Broker 才 unregister、detach Topic owner 并继续 MessageStore shutdown。超时报告保持 Topic owner、Store、OuterAPI 与 Rocks backend 不被提前关闭。
- `RocksDbBrokerConfigManager::replace_snapshot_with_version` 在同一 write batch 中删除 stale Topic key、写入当前 Topic rows 和 DataVersion；Topic/Subscription/Offset logical manager 不再独立 close backend，由 Broker aggregate owner 按 backend identity 去重并在三类 final persistence 后统一关闭。
- shutdown report 新增 `topic_config` typed component，记录 admission、pending、final persist、worker、registration quiescence、unregister、blocking-still-running 与 deadline；确定性 barrier 测试证明已接纳 registration 在 shutdown 前排空、关闭后命令被拒绝且 pending 归零。
- reviewed ArcMut baseline 保持 424 identities / 1,061 occurrences；production 255/571、test 155/450、compatibility 14/40、Broker production 133/263。本切片完成生命周期边界而不虚报 ArcMut 下降；`do_register_broker_all_inner` 的 1 个既有 occurrence 因同文件控制流调整发生一对一 fingerprint relocation，经 ADR-013 临时 approval 审核并更新 reviewed baseline，approval 不提交。

## M11-12bc2 验证

| 命令 | 结果 |
|---|---|
| Broker check / strict Clippy | 默认与 `rocksdb_store` library check、全 target/全 feature check，以及 Broker 全 target/全 feature strict Clippy 通过 |
| Coordinator / Topic manager focused tests | coordinator barrier/persistence 2/2、Topic manager default 10/10、Rocks restart/delete 2/2、atomic snapshot 1/1、metadata migration 2/2 通过；覆盖 admission/drain、pending 守恒、稳定最终写盘与 deleted Topic 不复活 |
| RocksDB specialized gates | Store strict Clippy 与 Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `python scripts/arc_mut_guard.py` 与 `--fixtures` 通过；baseline 数量不变，1 个 reviewed relocation 已更新 |
| runtime / architecture guards | enforcing runtime audit、AGENTS routing、134 个 guard tests、dependency fixtures/target/baseline、release 与 8-profile performance validation 全部通过 |
| 全特性 Broker lib | 最终 592 passed、25 failed、1 ignored；25 个失败与 main 已登记名单一致，首轮新增的 2 个 migration fixture 失败已修复并在最终全量中通过，未新增 baseline failure，因此全套仍如实记为基线复现而非通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc3 继续拆分 transaction bridge/listener capability 与其余 Broker admin/processor owner；75/82
总进度不变，Store、compatibility、stable/Miri/Loom/soak/SLO、动态 Kind/K3d/container、M10 固定硬件与 Human Gate
仍保持开放。

## M11-12bc3 实现

Transaction check runtime capability 随 Issue #8410 完成以下边界收敛：

- `DefaultTransactionalMessageCheckListener` 删除 `MessageStore` 泛型和完整 `ArcMut<BrokerRuntimeInner<MS>>` owner，只持 broker name、`ProducerChannelRegistry`、标准 `Arc<Broker2Client>` 与 runtime-owned leased TaskGroup；无 `ServiceContext` 的兼容构造只执行 inline send，不创建 current-runtime root fallback。
- checked-too-many-times message 的 Topic 创建、转换与 Store 写入回归 `DefaultTransactionalMessageService`；`TransactionalMessageBridge` 写路径使用独占 `&mut self`，删除 `put_message_return_result` 的 `mut_from_ref`。
- Broker shutdown 在 Topic coordinator admission close 与 MessageStore shutdown 之前依次停止 transaction check service、排空 listener task group、关闭 op-batch/service 与 metrics；四个 runtime slot 均以 `take` 释放，超时或不健康时保留 Topic owner 与 Store 并返回 typed component report。
- `BrokerStatsHandler` 删除无关 `MessageStore` 泛型和完整 runtime owner，只持标准 `Arc<BrokerStatsManager>`；`AdminBrokerProcessor` 删除未读取的 runtime 字段。
- reviewed baseline 从 424 identities / 1,061 occurrences 降至 418 / 1,051；production 从 255/571 降至 250/562，test 从 155/450 降至 154/449，compatibility 保持 14/40。Broker production 从 133/263 降至 128/254；净删除 5 个 production identity/9 occurrence 与 1 个 test identity/1 occurrence，无 relocation。

## M11-12bc3 验证

| 命令 | 结果 |
|---|---|
| Broker check / strict Clippy | default library 与 all-target/all-feature check 通过；Broker all-target/all-feature strict Clippy 通过 |
| transaction / capability focused tests | transaction 25/25、producer channel shared view 1/1、ownership/shutdown order 2/2、discard conversion 2/2 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 594 passed、25 failed、1 ignored；25 个失败与 main 已登记名单一致，较 M11-12bc2 增加 2 个通过测试，未新增 baseline failure，因此全套仍如实记为基线复现而非通过 |
| reviewed baseline / fixtures | `python scripts/arc_mut_guard.py` 与 `--fixtures` 通过；baseline 424/1,061→418/1,051，无 relocation |
| runtime / architecture guards | enforcing runtime audit、ArcMut 67/67、dependency fixtures/target/baseline、release、8-profile performance 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 和既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc4 继续拆分 transaction bridge 的 Store/offset capability 与其他 Broker admin/processor owner；
75/82 总进度不变，Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc4 实现

Transaction bridge runtime boundary 随 Issue #8412 完成以下 capability 收敛：

- `TransactionalMessageBridge` 与 `DefaultTransactionalMessageService` 删除完整 `BrokerRuntimeInner` owner；bridge 只持 store host、broker name、标准 `Arc<ConsumerOffsetManager>`、Topic registration、EscapeBridge 与显式 MessageStore capability，service 显式接收 Broker config 和文件保留时间。
- 新增 `TransactionTopicRegistration`，只组合 TopicConfig manager/coordinator、queue mapping、cloneable `BrokerOuterAPI`、store state-machine generation、shutdown 与 slave master-address capability；send-back/check-max Topic 创建继续保持 single/increment registration、权限投影、版本重采样、order config 和 HA master-address 更新语义。
- Broker runtime 以标准 `Arc` 发布 ConsumerOffsetManager 与 TopicQueueMappingManager 的同一代 owner；`BrokerOuterAPI`/`RpcClientImpl` 的 clone 共享既有 remoting client，`SlaveMasterAddress` 使用 `ArcSwapOption` 发布不可变地址代际，旧 reader 在替换后仍持有稳定值。
- 新增 `TransactionMessageStore` 作为直接 MessageStore 的过渡兼容 owner，将 remaining `ArcMut` 从完整 runtime bridge 隔离到 Store-only boundary。它不是 soundness 豁免，后续 Store 批次必须删除该 owner。
- reviewed ArcMut baseline 保持 418 identities / 1,051 occurrences：production 250/562、test 154/449、compatibility 14/40、Broker production 128/254。2 个 identity/3 个 occurrence 从 bridge 搬到显式 Store 边界，另有 2 个既有 occurrence 因相邻构造上下文变化发生一对一 fingerprint relocation；均经 ADR-013 临时 approval 审核，approval 不提交，未新增或隐藏债务。

## M11-12bc4 验证

| 命令 | 结果 |
|---|---|
| Broker check / strict Clippy | `cargo check -p rocketmq-broker --all-targets --all-features` 与 `cargo clippy -p rocketmq-broker --all-targets --all-features -- -D warnings` 通过 |
| transaction / capability focused tests | transaction 26/26、BrokerOuterAPI shared remoting client 1/1、Slave master-address immutable generations 1/1 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 598 passed、24 failed、1 ignored；24 项均属于 main 已登记的 25 项失败集合，本切片新增 3 个测试全部通过且失败集合未扩张，因此全套如实记为基线复现而非通过 |
| reviewed baseline / fixtures | `python scripts/arc_mut_guard.py` 与 `--fixtures` 通过；baseline 数量保持 418/1,051，reviewed identity/occurrence relocations 已更新 |
| runtime / architecture guards | enforcing runtime audit、ArcMut 67/67、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc5 收窄 BrokerRuntime aggregate/processor carrier 与其他 Broker leaf owner；75/82 总进度不变，
Store、compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc5 实现

Broker admin leaf capability 随 Issue #8414 完成以下 owner 收敛：

- `ProducerRequestHandler` 删除 `MessageStore` 泛型和完整 `ArcMut<BrokerRuntimeInner<MS>>` owner，只持既有 `ProducerChannelRegistry`。registry 新增 producer-table snapshot，并与 `ProducerManager::get_producer_table` 共用同一 live `DashMap` 采样函数；handler clone 不复制或冻结 producer 状态。
- `UpdateColdDataFlowCtrGroupConfigRequestHandler` 删除 `MessageStore` 泛型和完整 runtime owner，只持可选标准 `Arc<ColdDataCgCtrService>`；service 缺失、空 body、非法 property/threshold、remove no-op 与 JSON/wire response 语义保持不变。
- Broker runtime 将 ColdData service slot 改为 `Option<Arc<ColdDataCgCtrService>>`，保留 `Option<&ColdDataCgCtrService>` 借用 accessor 并新增 cloned capability handle；service 已有 RwLock/atomic 内部同步，空实现 lifecycle receiver 收窄为 `&self`，Pull reader 继续观察同一代状态。
- Admin processor 构造时只采样 producer registry 与 ColdData handle，不再把完整 runtime clone 传播给这两个 handler；源码边界测试固定两者不得重新引入 `BrokerRuntimeInner`、`ArcMut` 或 `MessageStore`。
- reviewed baseline 从 418 identities / 1,051 occurrences 降至 413 / 1,044；production 从 250/562 降至 246/556，test 从 154/449 降至 153/448，compatibility 保持 14/40。Broker production 从 128/254 降至 124/248；净删除 4 个 production identity/6 occurrence 与 1 个 test glob identity/1 occurrence，无 relocation。

## M11-12bc5 验证

| 命令 | 结果 |
|---|---|
| Broker check / strict Clippy | `cargo check -p rocketmq-broker --all-targets --all-features` 与 Broker all-target/all-feature strict Clippy 通过 |
| Producer / ColdData / admin focused tests | producer live registry 与 source boundary 2/2、ColdData round-trip/missing-service/source boundary 5/5、phase-5 admin wire regression 1/1 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 600 passed、25 failed、1 ignored；25 项属于 main 已登记失败集合，本切片新增 3 个测试全部通过且未新增失败名称，因此全套仍如实记为基线复现而非通过 |
| reviewed baseline / fixtures | `--prune-resolved` 精确删除 5 identity/7 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc6 拆除 MessageStore Schedule hook 对完整 BrokerRuntimeInner 的强保活环，只注入
MessageStoreConfig、TimerMessageStore 与 ScheduleMessageService capability；75/82 总进度不变。

## M11-12bc6 实现

Schedule hook runtime ownership 随 Issue #8416 完成以下 capability 收敛：

- `ScheduleMessageHook` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` owner，改持 `Arc<MessageStoreConfig>`、可选 `Arc<TimerMessageStore>` 与标准 `Arc<ScheduleMessageService<MS>>`。MessageStore 不再经 hook 强保活 Broker aggregate，`BrokerRuntimeInner → MessageStore → Hook → BrokerRuntimeInner` 环被拆除。
- `HookUtils::handle_schedule_message` 与 delay-level transform 删除 runtime 参数和泛型，只接收配置、timer 借用、当前最大延迟级别与消息；hook 每次执行仍从共享 Schedule service 读取实时 max level，保留 transaction gate、timer 校验/重写及固定延迟 Topic/queue/property 语义。
- timer wheel 已启用但 Timer store capability 缺失时返回既有 `WheelTimerNotEnable` 状态并记录警告，不再依赖 aggregate unchecked accessor；timer wheel 禁用时的既有拒绝状态保持不变。
- `register_message_store_hook` 在注册前采样三项能力，不再 `ArcMut::clone` runtime。源码 contract 禁止 hook/helper 重新引入完整 runtime/ArcMut，重复注册强引用计数回归直接证明 runtime root 不会被 Hook 保留。
- reviewed baseline 从 413 identities / 1,044 occurrences 降至 408 / 1,036；production 从 246/556 降至 242/549，test 从 153/448 降至 152/447，compatibility 保持 14/40。Broker production 从 124/248 降至 120/241；净删除 4 个 production identity/7 occurrence 与 1 个 test glob identity/1 occurrence，无 relocation。

## M11-12bc6 验证

| 命令 | 结果 |
|---|---|
| Broker check / strict Clippy | `cargo check -p rocketmq-broker --all-targets --all-features` 与 Broker all-target/all-feature strict Clippy 通过 |
| Schedule hook focused tests | HookUtils 9/9、source capability contract 1/1、runtime strong-count ownership 1/1 通过；覆盖 timer disabled/missing、timer transformation、实时 max delay 输入、目标重写和无 runtime 回边 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 605 passed、25 failed、1 ignored；25 项与 main 已登记失败集合一致，本切片新增 5 个测试全部通过且未新增失败名称，因此全套仍如实记为基线复现而非通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 精确删除 5 identity/8 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc7 继续收窄只读取少量能力的 Broker admin/processor leaf，随后进入 Store WAL/queue/timer/HA
owner 清零；75/82 总进度不变。

## M11-12bc7 实现

Store observer capability 随 Issue #8419 完成以下 owner 收敛：

- `BrokerStats` 删除 retained `ArcMut<MessageStore>`，只持可选标准 `Arc<BrokerStatsManager>`；统计快照、当前 put/get 计数与 manager 缺失时记录错误并返回 0 的语义不变。兼容构造接受 dereferenceable MessageStore handle，但只在构造边界提取 manager 后立即释放 Store handle；新组合根直接使用 `from_manager`。
- Broker 的 LocalFile 与 RocksDB MessageStore 初始化路径均直接注入同一代 `BrokerStatsManager` capability，不再为统计观察器复制完整 Store owner；泛型 `BrokerStats<MS>` 与既有 runtime slot/setter 保持兼容。
- `HAConnectionStateNotificationService` 删除具体 `LocalFileMessageStore` 与 `ArcMut` owner，只持标准 `Arc<MessageStoreConfig>`；slave 使用 HA client、master 遍历 connection 的路径选择语义不变。`DefaultHAService` 在组合根直接注入配置代际。
- 源码边界测试禁止两个 observer 重新保留不安全共享 handle 或具体 Store owner；统计 manager 行为、缺失 fallback 与三种 BrokerRole 路径选择均有聚焦回归。
- reviewed baseline 从 408 identities / 1,036 occurrences 降至 402 / 1,027；production 从 242/549 降至 236/540，test 保持 152/447，compatibility 保持 14/40。Store production 从 122/308 降至 116/299；净删除 6 个 production identity/9 occurrence，无 relocation。

## M11-12bc7 验证

| 命令 | 结果 |
|---|---|
| Store/Broker check 与 strict Clippy | 两 crate 的 all-target/all-feature check 与 strict Clippy 通过；Store/Broker `rocksdb_store` strict Clippy 通过 |
| observer capability focused tests | BrokerStats snapshot/missing-manager/source boundary 3/3，HA role selection/source boundary 2/2 通过 |
| Store/RocksDB 回归 | Store all-feature lib 497/497；RocksDB foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 605 passed、25 failed、1 ignored；25 项与 main 已登记失败集合一致，未新增失败名称，因此全套如实记为基线复现而非通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 精确删除 6 identity/9 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc8 继续收口 Store WAL/queue/timer/HA owner 或 Broker aggregate/leaf；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc8 实现

ConsumeQueueExt ownership 随 Issue #8421 完成以下边界收敛：

- `ConsumeQueueExt` 删除 `ArcMut<MappedFileQueue>` 与所有 `mut_from_ref` 访问，改持标准 `Arc<parking_lot::Mutex<MappedFileQueue>>`；load、recover、truncate、put、flush、destroy 与只读查询均在显式锁边界内访问 queue 状态。
- `put` 在同一个锁代际内完成容量检查、mapped-file 选择/创建与 append，避免多个共享 ext handle 在文件切换点观察并修改不同步的 queue 状态；I/O 和布局语义保持不变。
- 新增共享 ext 实例串行状态回归，验证 clone 后的实例观察相同 mapped-file queue；源码合同禁止该类型重新引入 `ArcMut`、`mut_from_ref` 或不安全共享 escape。
- reviewed baseline 从 402 identities / 1,027 occurrences 降至 396 / 1,019；production 从 236/540 降至 232/534，test 从 152/447 降至 150/445，compatibility 保持 14/40。Store production 从 116/299 降至 112/293；净删除 4 个 production identity/6 occurrence 与 2 个 test identity/2 occurrence，无 relocation。

## M11-12bc8 验证

| 命令 | 结果 |
|---|---|
| Store check / strict Clippy | Store all-target/all-feature check 与 strict Clippy 通过；Store/Broker `rocksdb_store` strict Clippy 通过 |
| ConsumeQueueExt focused tests | ConsumeQueueExt 6/6、SingleConsumeQueue 7/7 通过；覆盖共享 queue 状态、put/get、truncate/recover/load/destroy 与源码边界 |
| Store/RocksDB 回归 | Store all-feature lib 499/499；RocksDB foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 精确删除 6 identity/8 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc9 继续收窄 Store HA connection registry 或 Broker put-message preflight/leaf owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc9 实现

HA connection registry capability 随 Issue #8423 完成以下边界收敛：

- `HAService::get_connection_list` 删除，替换为 owned `HAAckedReplicaSnapshot` 列表与按 remote address 返回 scalar `HAConnectionState` 的窄能力；DefaultHAService registry 不再把 `ArcMut<GeneralHAConnection>` 暴露给 service consumer，General/AutoSwitch 只委托标量 API。
- GroupTransferService 直接等待 ack snapshot，不再先 try-lock、失败后克隆连接 owner；required-acks 与 sync-state-set 判定仍使用同一 broker-id/ack-offset 语义。
- HA notification master 路径不再先 `take` 请求后又从 slot 读取，非终态和地址不匹配请求保持注册；仅达到目标状态、允许的 shutdown、替换或 10 秒未发现连接超时时消费请求。slave 路径同时删除缺失 HA client 的 panic，并保留既有 Ready timeout/非 Ready 活跃时间语义。
- 连接 registry 的 ack/state 回归、notification pending/mismatch/shutdown/timeout 回归与源码 capability contract 已覆盖；reviewed baseline 从 396 identities / 1,019 occurrences 降至 394 / 1,014，production 从 232/534 降至 230/529，test 保持 150/445，compatibility 保持 14/40。Store production 从 112/293 降至 110/288；净删除 2 个 production identity/5 occurrence，无 relocation。

## M11-12bc9 验证

| 命令 | 结果 |
|---|---|
| Store check / strict Clippy | Store all-target/all-feature check 与 strict Clippy 通过；Store/Broker `rocksdb_store` strict Clippy 通过 |
| HA focused tests | notification 5/5、registry ack/state 1/1、group-transfer 5/5 通过；覆盖请求保留、地址匹配、目标/shutdown/timeout、owned snapshot 与 scalar state lookup |
| Store/RocksDB 回归 | Store all-feature lib 502/502；RocksDB foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 精确删除 2 identity/5 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc10 继续收窄 Broker put-message preflight/leaf 或 Store HA/Timer/WAL owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc10 实现

Put-message preflight capability 随 Issue #8425 完成以下边界收敛：

- `CheckBeforePutMessageHook` 删除完整 `ArcMut<MessageStore>` owner，改持 `PutMessagePreflight`；该能力只共享 shutdown、running flags 与 commit-log lock timestamp 三项原子状态，不允许访问、克隆或修改完整 Store。
- `MessageStore::put_message_preflight` 为 Local/Rocks/Generic Store 发布同一代 live 原子状态，自定义 Store 的默认实现 fail-closed；shutdown、不可写、flag diagnostics 与 OS page-cache busy 的既有判断语义保持。
- Broker 在可变注册 Hook 前先采样 preflight capability，注册完成后 Store 强引用计数保持不变，拆除 `MessageStore -> Hook -> MessageStore` 强引用环；Schedule/Batch hook 的注册与执行顺序不变。
- `LiteLifecycleManager` 的 max-offset/existence 查询从 `Option<&ArcMut<MS>>` 收窄为 `Option<&MS>`；新增 `message_store_ref` 只传播普通借用，Lite consumer lag、subscription、manager 与 POP Lite 调用方不再接触共享可变 Store carrier。
- reviewed baseline 从 394 identities / 1,014 occurrences 降至 389 / 1,007；production 从 230/529 降至 226/523，test 从 150/445 降至 149/444，compatibility 保持 14/40。Broker production 从 120/241 降至 116/235；净删除 4 个 production identity/6 occurrence 与 1 个 test identity/1 occurrence，无 relocation。

## M11-12bc10 验证

| 命令 | 结果 |
|---|---|
| Store/Broker check 与 strict Clippy | 两 crate check 通过；all-target/all-feature strict Clippy 通过；Store/Broker `rocksdb_store` strict Clippy 通过 |
| preflight/hook/Lite focused tests | Store preflight live-state/fail-closed 2/2、HookUtils 9/9、Hook registration Store strong-count 1/1、LiteLifecycle 1/1 通过 |
| Store/RocksDB 回归 | Store all-feature lib 504/504；RocksDB foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| `cargo test -p rocketmq-broker --all-features --lib -- --test-threads=1` | 605 passed、25 failed、1 ignored；25 项与 main 已登记失败名单一致，本切片聚焦测试全部通过且未新增失败名称，因此全套仍如实记为基线复现而非通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 审核候选精确删除 5 identity/7 occurrence；正式 baseline 补丁后 `python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| Rustdoc / public API snapshot | `cargo doc -p rocketmq-store --no-deps --all-features` 成功并复现 4 个既有 Rustdoc warning；31-package public API snapshot 为 `review-required`，基线自旧提交以来存在多包未归类漂移，本切片新增 Store public path 需随 M11 候选快照统一审核，未擅自重置基线 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc11 继续收窄 Broker aggregate/leaf 或 Store WAL/queue/timer/HA owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc11 实现

HA nested child ownership 随 Issue #8427 完成以下边界收敛：

- `GeneralHAClient` 的 Default/AutoSwitch enum payload 改为直接 owned child，并删除从未使用的 `Clone`；`AutoSwitchHAClient` 直接拥有 Default delegate，现有原子 broker id 与异步 address state 保持不变。
- `GeneralHAConnection` 的 Default/AutoSwitch optional child 改为直接 owned value；构造和替换不再添加嵌套 `ArcMut`，start/shutdown 继续由外层 `&mut self` 独占委托，其他状态查询继续只借用 `&self`。
- 外层 `DefaultHAService` connection registry、General service wrapper 与 connection task 的 `WeakArcMut<GeneralHAConnection>` 回指不在本切片提前改动，避免把 service/actor 生命周期与局部 child ownership 混为一次迁移。
- reviewed baseline 从 389 identities / 1,007 occurrences 降至 382 / 993；production 从 226/523 降至 219/509，test 保持 149/444，compatibility 保持 14/40。Store production 从 110/288 降至 103/274；净删除 7 个 production identity/14 occurrence，无 relocation。

## M11-12bc11 验证

| 命令 | 结果 |
|---|---|
| Store check / strict Clippy | `cargo check -p rocketmq-store --all-features` 与 Store all-target/all-feature strict Clippy 通过；Store/Broker `rocksdb_store` strict Clippy 通过 |
| HA focused tests | AutoSwitch client 2/2、auto-switch connection construction 1/1、connection-table lock-free shutdown 1/1、ACK callback 1/1 通过 |
| Store/RocksDB 回归 | Store all-feature lib 504/504；RocksDB foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 审核候选精确删除 7 identity/14 occurrence；正式 baseline 补丁后 `python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc12 继续收窄 Broker aggregate/leaf 或 Store WAL/queue/timer/HA owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc12 实现

ConsumerOrderInfo runtime capability 随 Issue #8429 完成以下边界收敛：

- `ConsumerOrderInfoManager` 删除完整 `ArcMut<BrokerRuntimeInner>` back-reference 与 `MessageStore` 泛型，改为只持存储根目录、标准 `Arc<TopicConfigManager>` 和共享 subscription-group live table；配置路径、topic existence 与 group existence 不再通过完整 Broker runtime 间接读取。
- Broker 初始化在 manager 边界注入同一代 Topic manager 与同一 live subscription-group table；配置路径与实时 topic/group 自动清理回归证明能力代际和原行为不变。
- 删除 4 个仓内无调用方的 `consumer_order_info_manager_mut`、`consumer_order_info_manager_unchecked_mut`、`consumer_order_info_manager_unchecked` 与 setter；共享只读 accessor 保留，consumer-order lock/info 行为不变。
- reviewed baseline 从 382 identities / 993 occurrences 降至 379 / 989；production 从 219/509 降至 217/506，test 从 149/444 降至 148/443，compatibility 保持 14/40。Broker production 从 116/235 降至 114/232；净删除 2 个 production identity/3 occurrence 与 1 个 test identity/1 occurrence，无 relocation。

## M11-12bc12 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；ConsumerOrderInfoManager 7/7 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 607 passed、25 failed、1 ignored；25 项与 main 已登记失败名单一致，较上一切片增加的 2 个通过项均为本切片回归，因此全套仍如实记为基线复现而非通过 |
| RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 审核候选精确删除 3 identity/4 occurrence；正式 baseline 补丁后 `python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc13 继续收窄 Broker aggregate/leaf 或 Store WAL/queue/timer/HA owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc13 实现

Broker orphan V2 migration example 随 Issue #8431 完成以下边界清理：

- 删除 `rocketmq-broker/src/processor_v2_migration_example.rs`；该 tracked standalone source 从未加入 Broker module tree，Cargo/测试从未编译或调用其中的 processor wrapper、dispatcher 或伪 benchmark，因此删除不改变 Broker runtime wiring。
- `rocketmq-remoting/src/runtime/processor_v2.rs`、`rocketmq-remoting/examples/processor_v2_complete_example.rs` 与 `rocketmq-remoting/tests/processor_v2_tests.rs` 保持 V2 implementation、完整 example 和 executable integration coverage 的 canonical owner，不复制维护第二套 Broker 示例。
- reviewed baseline 从 379 identities / 989 occurrences 降至 376 / 980；production 从 217/506 降至 215/499，test 从 148/443 降至 147/441，compatibility 保持 14/40。Broker production 从 114/232 降至 112/225；净删除 2 个 production identity/7 occurrence 与 1 个 test identity/2 occurrence，无 relocation。

## M11-12bc13 验证

| 命令 | 结果 |
|---|---|
| Broker / Remoting V2 | `cargo check -p rocketmq-broker --all-features` 与 Remoting complete example check 通过；`processor_v2_tests` 7/7 通过、1 个手工 zero-allocation verification 保持 ignored |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 审核候选精确删除 3 identity/9 occurrence；正式 baseline 补丁后 `python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc14 继续收窄 Broker aggregate/leaf 或 Store WAL/queue/timer/HA owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc14 实现

TopicRouteInfo runtime capability 随 Issue #8433 完成以下边界收敛：

- `TopicRouteInfoManager` 删除完整 `ArcMut<BrokerRuntimeInner>` back-reference 与 `MessageStore` 泛型，改为只持共享 `BrokerOuterAPI`、Namesrv 轮询间隔和可选父 `TaskGroup`；route refresh 不再借由完整 Broker runtime 取得三项能力。
- Broker 初始化注入同一 Remoting client generation 的 `BrokerOuterAPI` clone、配置间隔与 Broker service task-group parent；无 `ServiceContext` 时仍从 ambient Tokio runtime 创建兼容 root，保留原失败告警、幂等启动、cancellation 与 5 秒有界 shutdown 语义。
- 删除仓内无调用方的 unchecked mutable accessor 与 setter；测试 route table 写入改经共享 accessor，标准 `Arc<DashMap>` clone 仍观察同一 live route state。
- reviewed baseline 从 376 identities / 980 occurrences 降至 374 / 977；production 从 215/499 降至 213/496，test 保持 147/441，compatibility 保持 14/40。Broker production 从 112/225 降至 110/222；净删除 2 个 production identity/3 occurrence，无 relocation。

## M11-12bc14 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；TopicRouteInfoManager lifecycle/parent/shared-table 3/3 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 607 passed、25 failed、1 ignored；25 项与 main 已登记失败名单一致，TopicRouteInfoManager 聚焦测试全部通过且未新增失败名称，因此全套仍如实记为基线复现而非通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 审核候选精确删除 2 identity/3 occurrence；正式 baseline 补丁后 `python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc15 继续收窄 Broker aggregate/leaf 或 Store WAL/queue/timer/HA owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc15 实现

MessageArrivingListener runtime cycle 随 Issue #8435 完成以下边界收敛：

- Store-owned `NotifyMessageArrivingListener` 删除完整 `ArcMut<BrokerRuntimeInner>` back-reference，改为 Pull request hold service、POP processor 与 Notification processor 三项标准 `Weak` handle；listener 无法再读取或保活其他 Broker runtime capability。
- listener 注册从 Pull hold 初始化后移动到三项 owner 均已建立之后；arrival fan-out 逐项 upgrade，正常运行时仍调用同一三个通知 API，teardown 后的 late notification 对已释放 owner 安全跳过而不再经 unchecked accessor panic。
- 新增真实 Broker runtime 回归：初始化 MessageStore listener 后采样 runtime strong count，清除 listener 后计数保持完全不变；旧完整 runtime owner 会使该断言下降 1。
- reviewed baseline 从 374 identities / 977 occurrences 降至 372 / 974；production 从 213/496 降至 211/493，test 保持 147/441，compatibility 保持 14/40。Broker production 从 110/222 降至 108/219；净删除 2 个 production identity/3 occurrence，无 relocation。

## M11-12bc15 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；listener runtime strong-count 回归 1/1 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 608 passed、25 failed、1 ignored；25 项与 main 已登记失败名单一致，新增 listener ownership 回归通过且未新增失败名称，因此全套仍如实记为基线复现而非通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 审核候选精确删除 2 identity/3 occurrence；正式 baseline 补丁后 `python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc16 继续收窄 Broker aggregate/leaf 或 Store WAL/queue/timer/HA owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc16 实现

ClientHousekeeping runtime capability 随 Issue #8438 完成以下边界收敛：

- `ClientHousekeepingService` 删除完整 `ArcMut<BrokerRuntimeInner>` back-reference 与 `MessageStore` 泛型，改持仅暴露 inactive scan/channel close 的 Producer/Consumer narrow handle、标准 `Arc<BrokerStatsManager>` 与可选父 `TaskGroup`；Remoting channel listener 无法再读取或保活其他 Broker/Store capability。
- Narrow handle 只共享 manager 内部标准 `Arc`/`Weak` live state，完整 Producer/Consumer manager 没有获得通用 `Clone` 接口；周期 inactive scan 与 channel connect/close/exception/idle 仍作用于同一连接表、change listener 和统计代际。Broker service task group 继续作为 parent，无 `ServiceContext` 时保留 ambient Tokio runtime fallback。
- lifecycle/idempotent shutdown、parent task-group 与 runtime strong-count 回归 3/3 通过；新 ownership 回归证明构造和释放 service 都不改变 Broker runtime root 强引用计数。
- reviewed baseline 从 372 identities / 974 occurrences 降至 369 / 970；production 从 211/493 降至 209/490，test 从 147/441 降至 146/440，compatibility 保持 14/40。Broker production 从 108/219 降至 106/216；净删除 2 个 production identity/3 occurrence 与 1 个 test identity/1 occurrence，无 relocation。

## M11-12bc16 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；housekeeping lifecycle/parent/runtime strong-count 回归 3/3 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 609 passed、25 failed、1 ignored；25 项与 main 已登记失败名单一致，新增 ownership 回归通过且未新增失败名称，因此全套仍如实记为基线复现而非通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 审核候选精确删除 3 identity/4 occurrence；正式 baseline 补丁后 `python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation/临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check`、`git diff --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

下一子切片 M11-12bc17 继续收窄 Broker aggregate/leaf 或 Store WAL/queue/timer/HA owner；75/82 总进度不变，
compatibility、stable/Miri/Loom/soak/SLO 与完整候选快照 Gate 仍保持开放。

## M11-12bc17 实现

Read-only Broker diagnostics runtime borrow 随 Issue #8440 完成以下边界收敛：

- Admin dispatch 复用 broker-config handler 已登记的 runtime owner；`GetBrokerHaStatusHandler` 与 `BrokerEpochCacheHandler` 删除各自的完整 `ArcMut<BrokerRuntimeInner>` field、Clone 与 struct-level `MessageStore` 泛型，变为无状态 leaf，且不在父层新增 owner。
- Admin dispatch 在单次请求期间从该现有 owner 取得普通 `&BrokerRuntimeInner` 共享借用并传给两个 leaf；HA status 仍只读取 MessageStore HA projection，epoch cache 仍只读取 controller config、replica epoch、broker identity 与当前 physical offset，响应和缺失 Store/HA/controller-mode 错误语义不变。
- HA status success/missing-store 与 runtime strong-count 回归 3/3 通过；构造和释放两个 stateless handler 均不改变 runtime root 强引用计数。
- reviewed baseline 从 369 identities / 970 occurrences 降至 364 / 963；production 从 209/490 降至 205/484，test 从 146/440 降至 145/439，compatibility 保持 14/40。Broker production 从 106/216 降至 102/210；净删除 4 个 production identity/6 occurrence 与 1 个 test identity/1 occurrence，无 relocation。

## M11-12bc17 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；HA status success/missing-store/runtime strong-count 回归 3/3 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 607 passed、28 failed、1 ignored；其中 25 项与 main 已登记失败名单一致，首轮另有 3 个 controller-mode 用例受固定端口占用影响；`three_controller_two_broker` 串行重跑恢复 3/4，剩余 failover/rejoin 用例单独再跑仍在既有 namesrv/store/HA slave-view 收敛等待点超时。本切片两个诊断 handler 的聚焦回归全部通过且不参与 controller rejoin 路径，因此全套如实记为未通过，不把超时视为本切片通过证据 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 补丁精确删除 5 identity/7 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc18 实现

Broker HA control runtime borrow 随 Issue #8442 完成以下边界收敛：

- `ResetMasterFlushOffsetHandler` 与 `UpdateBrokerHaHandler` 删除各自的完整 `ArcMut<BrokerRuntimeInner>` field、Clone 与 struct-level `MessageStore` 泛型，改为无状态 leaf；Admin 父层不新增 runtime owner。
- Admin dispatch 在 reset-master-flush-offset 与 exchange-broker-HA-info 请求期间，从 broker-config handler 已登记的 owner 取得普通 `&BrokerRuntimeInner` 共享借用；master/slave 分支、MessageStore offset 更新、HA master address 更新/返回及缺失 Store 语义保持不变。
- ownership 回归覆盖四个 stateless runtime-borrow handler，构造和释放均不改变 runtime root 强引用计数；HA status success/missing-store 回归继续通过。
- reviewed baseline 从 364 identities / 963 occurrences 降至 360 / 957；production 从 205/484 降至 201/478，test 保持 145/439，compatibility 保持 14/40。Broker production 从 102/210 降至 98/204；净删除 4 个 production identity/6 occurrence，无 relocation。

## M11-12bc18 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；四个 stateless handler ownership 与 HA status success/missing-store 回归 3/3 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 607 passed、28 failed、1 ignored；其中 25 项与 main 已登记失败名单一致，首轮另有 3 个 controller-mode 用例受固定端口占用影响；`three_controller_two_broker` 串行重跑恢复 3/4，剩余 failover/rejoin 用例仍在既有 namesrv/store/HA slave-view 收敛等待点超时。本切片聚焦回归通过且两个 handler 不参与 controller rejoin orchestration，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均只删除 4 identity/6 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc19 实现

Broker batch lock runtime borrow 随 Issue #8444 完成以下边界收敛：

- `BatchMqHandler` 删除完整 `ArcMut<BrokerRuntimeInner>` field、Clone 与 struct-level `MessageStore` 泛型，改为无状态 leaf；Admin dispatch 在 lock/unlock 请求期间从 broker-config handler 已登记的 owner 取得普通 `&BrokerRuntimeInner` 借用。
- strict-lock fan-out 只 clone 标准 `BrokerOuterAPI` 窄能力进入每个副本 future，不再 clone 或捕获完整 runtime；local lock、replica quorum、2 秒 fan-out timeout、远端失败降级与 unlock 语义保持不变。
- ownership 回归覆盖五个 stateless runtime-borrow handler，构造和释放均不改变 runtime root 强引用计数；HA status success/missing-store 回归继续通过。
- reviewed baseline 从 360 identities / 957 occurrences 降至 358 / 954；production 从 201/478 降至 199/475，test 保持 145/439，compatibility 保持 14/40。Broker production 从 98/204 降至 96/201；净删除 2 个 production identity/3 occurrence，无 relocation。

## M11-12bc19 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；五个 stateless handler ownership 与 HA status success/missing-store 回归 3/3 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 首轮 608 passed、27 failed、1 ignored；25 项与 main 已登记失败名单一致，另 2 个 controller-mode 用例受固定端口占用影响；`three_controller_two_broker` 串行重跑 4/4 全部通过。BatchMq 相关 dispatch/ownership 路径未出现新增失败，因此全套仍如实记为基线失败复现而非通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均只删除 2 identity/3 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc20 实现

Subscription-group Admin runtime borrow 随 Issue #8446 完成以下边界收敛：

- `SubscriptionGroupHandler` 删除完整 `ArcMut<BrokerRuntimeInner>` field、Clone 与 struct-level `MessageStore` 泛型，改为无状态 leaf；Admin 父层不新增 owner。
- Admin dispatch 对 create/list/delete/forbidden 写请求从 broker-config handler 已登记的 owner 取得请求期 `&mut BrokerRuntimeInner` 独占借用，配置查询使用普通共享借用；handler 不能在请求之外保活或访问 runtime。
- 删除该模块中从未被 Admin dispatch 调用、且已由 canonical `BatchMqHandler` 承担的重复 `unlock_batch_mq` 与相关 imports；subscription-group create/list/delete/forbidden/offset-cleanup 响应路径保持原语义。
- ownership 回归覆盖六个 stateless runtime-borrow handler并保持 3/3 通过；subscription-group 聚焦回归 2/3 通过，唯一失败是已登记的 delete-offset-cleanup 基线断言（reset offset 8 未清为 -1）。
- reviewed baseline 从 358 identities / 954 occurrences 降至 355 / 950；production 从 199/475 降至 197/472，test 从 145/439 降至 144/438，compatibility 保持 14/40。Broker production 从 96/201 降至 94/198；净删除 2 个 production identity/3 occurrence 与 1 个 test identity/1 occurrence，无 relocation。

## M11-12bc20 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；六个 stateless handler ownership 与 HA status 回归 3/3 通过；subscription-group 聚焦回归 2/3 通过，唯一失败为已登记 delete-offset-cleanup 基线断言；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 609 passed、26 failed、1 ignored；登记基线中的 24 项本轮失败、1 个间歇项本轮通过，另 2 个 controller-mode 用例受固定端口占用影响；`three_controller_two_broker` 串行重跑 3/4，剩余 failover/rejoin 用例仍在既有 slave-view 收敛等待点超时。subscription-group 失败名称与断言未变化，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均只删除 3 identity/4 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc21 实现

Message-related Admin runtime borrow 随 Issue #8448 完成以下边界收敛：

- `MessageRelatedHandler` 删除完整 `ArcMut<BrokerRuntimeInner>` field 与 struct-level `MessageStore` 泛型，改为无状态 leaf；Admin 父层不新增 runtime owner。
- Admin dispatch 对 search-offset、query-consume-queue 和 POP rollback 请求从 broker-config handler 已登记的 owner 取得普通 `&BrokerRuntimeInner` 共享借用；仅 resume-check-half-message 重入写 Store 时取得请求期 `&mut BrokerRuntimeInner` 独占借用。
- 静态主题 search-offset 重写沿用调用方传入的同一共享借用；本地 Store 查询、远端 Broker RPC、consumer/filter 查询、POP service restart 与半消息重新入队语义保持不变。
- 消息处理聚焦回归 4/4、stateless handler runtime strong-count 回归通过；构造和释放 handler 不改变 runtime root 强引用计数。
- reviewed baseline 从 355 identities / 950 occurrences 降至 353 / 947；production 从 197/472 降至 195/469，test 保持 144/438，compatibility 保持 14/40。Broker production 从 94/198 降至 92/195；净删除 2 个 production identity/3 occurrence，无 relocation。

## M11-12bc21 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；message-related 回归 4/4 与 stateless handler ownership 回归通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 610 passed、25 failed、1 ignored；25 项均属于 main 已登记的 lifecycle/Lite/subscription 基线失败集合，controller-mode 4/4 本轮全部通过；message-related 聚焦回归无新增失败，因此全套仍如实记为基线失败复现而非通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均只删除 2 identity/3 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc22 实现

Broker offset Admin runtime borrow 随 Issue #8450 完成以下边界收敛：

- `OffsetRequestHandler` 删除完整 `ArcMut<BrokerRuntimeInner>` field、Clone 与 struct-level `MessageStore` 泛型，改为无状态 leaf；Admin 父层不新增 runtime owner。
- Admin dispatch 对 max/min/earliest offset、delay offset、subscription snapshot、expired consume-queue/commitlog cleanup 请求从 broker-config handler 已登记的 owner 取得普通 `&BrokerRuntimeInner` 共享借用；unsupported RocksDB CQ progress 路径无需 runtime。
- static-topic max/min/earliest 重写沿用调用方传入的同一共享借用；本地 Store 查询、远端 Broker RPC、cleanup trigger 与响应语义保持不变。
- offset 聚焦回归 5/5、stateless handler runtime strong-count 回归通过；构造和释放 handler 不改变 runtime root 强引用计数。
- reviewed baseline 从 353 identities / 947 occurrences 降至 351 / 944；production 从 195/469 降至 193/466，test 保持 144/438，compatibility 保持 14/40。Broker production 从 92/195 降至 90/192；净删除 2 个 production identity/3 occurrence，无 relocation。

## M11-12bc22 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；offset 回归 5/5 与 stateless handler ownership 回归通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 首轮 608 passed、27 failed、1 ignored；25 项属于 main 已登记的 lifecycle/Lite/subscription 基线失败集合，另 2 个 controller-mode 用例受固定端口 20011 占用影响；`three_controller_two_broker` 串行重跑 3/4，唯一失败仍是已登记的 namesrv/store/HA slave-view 收敛超时。offset 聚焦回归无新增失败，因此全套仍如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均只删除 2 identity/3 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc23 实现

Minimum-broker Admin runtime borrow 随 Issue #8452 完成以下边界收敛：

- `NotifyMinBrokerChangeIdHandler` 删除完整 `ArcMut<BrokerRuntimeInner>` field 与 struct-level `MessageStore` 泛型，只保留标准 `Arc<RocketMQTokioRwLock<_>>` 管理的 broker-id/address 状态；Admin 父层不新增 runtime owner。
- Admin dispatch 对 minimum-broker 角色切换请求从 broker-config handler 已登记的 owner 取得请求期 `&mut BrokerRuntimeInner` 独占借用，并贯穿 special-service、master offline 和 master online 路径。
- 两个 `mut_from_ref` 调用被普通独占借用替代；channel close、slave master address、Store HA/master address、flush offset、HA wakeup 与 pull-hold notification 语义保持不变。
- stateless-runtime handler strong-count 回归通过；构造和释放 handler 不改变 runtime root 强引用计数。
- reviewed baseline 从 351 identities / 944 occurrences 降至 348 / 939；production 从 193/466 降至 190/461，test 保持 144/438，compatibility 保持 14/40。Broker production 从 90/192 降至 87/187；净删除 3 个 production identity/5 occurrence，无 relocation。

## M11-12bc23 验证

| 命令 | 结果 |
|---|---|
| Broker check / ownership / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；stateless-runtime handler ownership 回归通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 610 passed、25 failed、1 ignored；25 项均属于 main 已登记的 lifecycle/Lite/subscription 基线失败集合，minimum-broker 路径未出现新增失败；`three_controller_two_broker` 串行重跑 3/4，唯一失败仍是已登记的 namesrv/store/HA slave-view 收敛超时，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均只删除 3 identity/5 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc24 实现

Consumer Admin runtime borrow 随 Issue #8454 完成以下边界收敛：

- `ConsumerRequestHandler` 删除完整 `ArcMut<BrokerRuntimeInner>` field、Clone 与 struct-level `MessageStore` 泛型，改为无状态 leaf；Admin 父层不新增 runtime owner。
- Admin dispatch 对 consumer connection/stats/status/subscription/time-span、request-mode、running-info 与 offset clone 请求从 broker-config handler 已登记的 owner 取得请求期 `&BrokerRuntimeInner` 共享借用；仅 reset-offset 的 Broker-to-client 写边界取得 `&mut BrokerRuntimeInner` 独占借用。
- connection、consume stats、direct consume、reset/status、subscription、time-span 与 offset clone 聚焦回归 9/9 通过；stateless-runtime handler strong-count 回归通过。
- reviewed baseline 从 348 identities / 939 occurrences 降至 346 / 936；production 从 190/461 降至 188/458，test 保持 144/438，compatibility 保持 14/40。Broker production 从 87/187 降至 85/184；净删除 2 个 production identity/3 occurrence，无 relocation。

## M11-12bc24 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused tests / ownership / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；consumer Admin 聚焦回归 9/9 与 stateless-runtime handler ownership 回归通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 610 passed、25 failed、1 ignored；consumer Admin 聚焦测试无新增失败，失败仍属于 main 已登记的 lifecycle/Lite/subscription/controller 动态基线集合；`three_controller_two_broker` 串行重跑 3/4，唯一失败仍是已登记的 namesrv/store/HA slave-view 收敛超时，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均只删除 2 identity/3 occurrence；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、父层新增 owner 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc25 实现

Store flush wakeup capability 随 Issue #8456 完成以下边界收敛：

- `CommitRealTimeService` 删除完整 `WeakArcMut<DefaultFlushManager>` back-reference 和 late setter，改持只包含 group-commit/flush-realtime 可选 `Notify` 与 `flush_commit_log_timed` policy 的 `FlushWakeup`。
- commit worker 成功后直接调用窄唤醒能力；sync flush 唤醒 group-commit，async untimed 唤醒 flush-realtime，async timed 留给周期 worker，不再升级完整 manager。
- `CommitLog::start` 删除 `ArcMut::downgrade` 与注入路径，并删除无其他调用方的 commit-service accessors；worker 的 `CancellationToken`、`TaskGroup`、start/shutdown 顺序保持不变。
- reviewed baseline 从 346 identities / 936 occurrences 降至 344 / 932；production 从 188/458 降至 186/454，test 保持 144/438，compatibility 保持 14/40。Store production 从 103/274 降至 101/270；净删除 2 个 production identity/4 occurrence。相邻 import 删除和测试扩展导致 2 个保留 import occurrence 指纹一对一变化，已按实际扫描审核更新，无新增 identity。

## M11-12bc25 验证

| 命令 | 结果 |
|---|---|
| Store focused / all-feature check / lib / strict Clippy | flush wakeup 三类 policy 回归 3/3；`cargo check -p rocketmq-store --all-features`、507/507 all-feature library tests 与 all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib 回归 | 611 passed、24 failed、1 ignored；失败仍全部属于 main 已登记的 lifecycle/Lite/subscription 动态基线，无 flush/commit 新失败；`three_controller_two_broker` 串行重跑 3/4，唯一失败仍是已登记的 rejoin namesrv/store/HA slave-view 收敛超时，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选精确从 346/936 降至 344/932；正式 baseline 删除 2 identity/4 occurrence并审核更新 2 个保留 import 指纹；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无新增 identity或临时 approval 文件 |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile performance、architecture 60/60 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc26 实现

Store HA replication state publication 随 Issue #8459 完成以下边界收敛：

- `CommitLogRuntimeState::confirm_offset` 从普通 `i64` 改为 `AtomicI64`，共享读写使用 `SeqCst`；原有可变 setter facade 保持兼容并委托给共享发布入口。
- `CommitLog` 与 `LocalFileMessageStore` 增加 crate-private confirm/epoch/state-machine 窄发布入口；auto-switch HA service 的 sync-state、slave ack、connection removal、epoch transition 与 role-change 路径不再取得完整 Store 可变引用。
- HA reader 继续先将 master confirm offset clamp 到本地 `[min_phy_offset, max_phy_offset]` 再原子发布；confirm offset 允许在角色切换时下降，因此没有错误地使用单调 `fetch_max`。
- epoch transition 仍只在 `Advanced` 时按 state-machine version、epoch start offset 的既有顺序发布；checkpoint 更新与公开 API 签名保持不变。
- reviewed baseline 从 344 identities / 932 occurrences 降至 342 / 926；production 从 186/454 降至 184/448，test 保持 144/438，compatibility 保持 14/40。Store production 从 101/270 降至 99/264；净删除 2 个 production identity/6 occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc26 验证

| 命令 | 结果 |
|---|---|
| Store Local / Store focused、all-feature check 与 strict Clippy | shared confirm offset 下降回归 1/1、HA confirm/role/epoch 回归 5/5；`cargo check -p rocketmq-store --all-features` 与 Store Local/Store all-target/all-feature strict Clippy 通过 |
| Store Local / Store all-feature lib | Store Local 186/186、Store 507/507 通过 |
| Broker all-feature lib 回归 | 610 passed、25 failed、1 ignored；失败集中于 lifecycle/Lite/subscription 动态基线，无 HA/confirm/epoch 新失败；独立 lifecycle probe 仍失败于 consumer-offset/subscription-group shutdown 健康度，`three_controller_two_broker` 串行重跑 4/4 通过，因此全量套件如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均从 344/932 精确降至 342/926；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、architecture 60/60 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc27 实现

Broker controller role-change notification 随 Issue #8461 完成以下边界收敛：

- `NotifyBrokerRoleChangeHandler` 改为无状态、非泛型 leaf，删除完整 `ArcMut<BrokerRuntimeInner>` 字段、constructor 注入和模块 import。
- `AdminBrokerProcessor` 复用同一聚合内已有的 `BrokerConfigRequestHandler` runtime owner；新增窄角色变更委托，不增加新的 owner、`ArcMut` occurrence 或父层 carrier。
- 通知 handler 继续负责 header/body 解码、channel remote address 到 controller leader address 的映射；controller 未初始化仍返回 Success，apply error 仍转换为 `SystemError`。
- `BrokerRuntimeInner::apply_controller_role_change` 的角色切换、Store/特殊服务处理和必要时 NameServer 注册顺序保持不变。
- 所有权回归覆盖无 controller 初始化请求，证明 handler 构造和请求完成后 runtime strong count 不增加。
- reviewed baseline 从 342 identities / 926 occurrences 降至 340 / 923；production 从 184/448 降至 182/445，test 保持 144/438，compatibility 保持 14/40。Broker production 从 85/184 降至 83/181；净删除 2 个 production identity/3 occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc27 验证

| 命令 | 结果 |
|---|---|
| Broker focused / all-feature check / strict Clippy | uninitialized-controller Success 与 owner strong-count 回归 1/1；`cargo check -p rocketmq-broker --all-features` 和 all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib / controller 回归 | 611 passed、25 failed、1 ignored；失败仍集中于 lifecycle/Lite/subscription 与 controller rejoin 动态基线，无 role-change handler 新失败；`three_controller_two_broker` 串行重跑 3/4，唯一失败仍是 rejoin namesrv/store/HA slave-view 收敛超时，因此全套如实记为未通过 |
| Store / RocksDB 专项 | Store all-feature lib 507/507；Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式补丁均从 342/926 精确降至 340/923；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、architecture 60/60 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc28 实现

Store HA connection weak self-cycle 随 Issue #8464 完成以下边界收敛：

- `HAConnection::start` 删除 `WeakArcMut<GeneralHAConnection>` 参数，Default/General/AutoSwitch connection 不再要求 caller 在启动前创建完整 connection 的弱自引用。
- read/write worker 改持 cloneable crate-private `HAConnectionRuntimeHandle`，只保留 connection id、remote address、共享 connection state 和 auto-switch 可选 slave broker id，不传播完整 connection owner。
- `AutoSwitchHAConnection` 以标准 `Arc<AtomicI64>` 共享 slave broker id；reader 收到 controller-mode broker id 后通过窄 handle 发布，wrapper 与 service callback 读取同一原子值。
- Default HA service 新增 runtime-handle ack/caught-up/removal 路径；状态通知、connection table 删除和 auto-switch sync-state 更新时间只消费窄 handle 或 slave id/ack 标量。
- flow monitor shutdown、TaskGroup worker 所有权、connection state 先置为 Shutdown 再通知/移除以及现有 add/destroy compatibility facade 顺序保持不变。
- reviewed baseline 从 340 identities / 923 occurrences 降至 331 / 907；production 从 182/445 降至 174/432，test 从 144/438 降至 143/435，compatibility 保持 14/40。Store production 从 99/264 降至 91/251；净删除 8 个 production identity/13 occurrence与 1 个 test identity/3 occurrence；1 个保留 import occurrence 经同位置指纹审核更新，无 relocation、新增 identity 或临时 approval。

## M11-12bc28 验证

| 命令 | 结果 |
|---|---|
| Store focused / all-feature check / strict Clippy | Default HA service 11/11；`cargo check -p rocketmq-store --all-features` 与 Store all-target/all-feature strict Clippy 通过 |
| Store all-feature lib | 507/507 通过；HA connection 启停、auto-switch slave id/ack、sync-state 与 connection destruction 回归无新增失败 |
| Broker all-feature lib / controller 回归 | 609 passed、27 failed、1 ignored；失败仍集中于 lifecycle/Lite/subscription 与 controller 动态基线，无 HA runtime-handle 新失败；`three_controller_two_broker` 串行重跑 3/4，唯一失败仍是 rejoin namesrv/store/HA slave-view 收敛超时，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | reviewed 候选与正式最小补丁均从 340/923 精确降至 331/907，1 个保留 import occurrence 经同位置指纹审核更新；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、architecture 60/60 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc29 实现

Store WAL sync-flush enqueue 随 Issue #8467 完成以下边界收敛：

- `GroupCommitService::put_request` 从 `&mut self` 收窄为 `&self`；该路径只读取 cancellation token 与 sender、等待 bounded-channel send/cancellation，并更新原子统计，不修改 service 字段。
- `DefaultFlushManager::handle_disk_flush_shared` 以共享 receiver 承载原 sync/async flush 逻辑；公开 `FlushManager::handle_disk_flush(&mut self, ...)` 签名保持兼容并直接委托 shared 实现。
- `CommitLog::handle_disk_flush(&self, ...)` 直接调用 shared 实现，删除 WAL put 热路径唯一的 `mut_from_ref`；observability latency 记录位置与状态返回不变。
- sync-flush enqueue 前后的 cancellation 快速失败、bounded backpressure、成功后统计、completion timeout/error mapping，以及 async wakeup 分支保持不变。
- flush manager start/shutdown、sender 发布和 worker TaskGroup 仍由独占可变 lifecycle owner 管理；不增加锁、后台任务或跨 `.await` 同步 guard。
- reviewed baseline 从 331 identities / 907 occurrences 降至 330 / 906；production 从 174/432 降至 173/431，test 保持 143/435，compatibility 保持 14/40。Store production 从 91/251 降至 90/250；净删除 1 个 production identity/1 occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc29 验证

| 命令 | 结果 |
|---|---|
| Store focused / all-feature check / strict Clippy | shared receiver sync-flush 成功完成并保留 missing-service timeout 的聚焦回归 1/1；`cargo check -p rocketmq-store --all-features` 与 Store all-target/all-feature strict Clippy 通过 |
| Store all-feature lib | 507/507 通过；sync/async flush、group-commit shutdown 与 CommitLog 行为无新增失败 |
| Broker all-feature lib | 611 passed、25 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 动态基线，flush/HA/RocksDB/controller 均无新失败，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式最小补丁均从 331/907 精确降至 330/906；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、architecture 60/60 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc30 实现

Store HA replication-state callback 随 Issue #8469 完成以下边界收敛：

- `AutoSwitchHAService` 以标准 `Arc<ReplicationStateRoot>` 共享复制状态，不再把完整 service 通过 `WeakArcMut` 回注到 `DefaultHAService`。
- `DefaultHAService` 的 connection added、ack、caught-up 与 removed callback 直接消费窄 replication-state capability，并复用既有 Store 只读访问完成 runtime snapshot、confirm-offset 计算和发布。
- caught-up timestamp、sync-state expand/remove、shutdown short circuit、connection registry 与 confirm-offset 更新顺序保持不变；Default HA 模式不持有 auto-switch replication state。
- 所有权回归断言 auto-switch service 的 `weak_count()` 保持为零，证明初始化和 callback 不再建立旧的完整 service 弱回边。
- reviewed baseline 从 330 identities / 906 occurrences 降至 328 / 899；production 从 173/431 降至 171/424，test 保持 143/435，compatibility 保持 14/40。Store production 从 90/250 降至 88/243；净删除 2 个 production identity/7 occurrences，4 个保留 occurrence 经同 item 一对一指纹审核更新，production `WeakArcMut` 已清零。

## M11-12bc30 验证

| 命令 | 结果 |
|---|---|
| Store focused / all-feature check / strict Clippy | HA callback 聚焦回归 3/3；`cargo check -p rocketmq-store --all-features` 与 Store all-target/all-feature strict Clippy 通过 |
| Store all-feature lib | 507/507 通过；auto-switch ack/caught-up/remove、sync-state 与 confirm-offset 行为无新增失败 |
| Broker all-feature lib | 610 passed、26 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription/controller 动态基线，无 HA replication-state 新失败，因此全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 从 330/906 精确降至 328/899，4 个保留 occurrence 通过同 item 一对一审核；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 ArcMut guard tests 通过，无新增 identity 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、127/127 guard tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc31 实现

Broker Topic Admin request borrow 随 Issue #8471 完成以下边界收敛：

- `TopicRequestHandler<MS>` 改为无状态、非泛型 `TopicRequestHandler`，删除完整 `ArcMut<BrokerRuntimeInner>` field、constructor clone 与模块 import。
- `AdminBrokerProcessor` 复用 `BrokerConfigRequestHandler` 已有 runtime owner：topic 查询与 clean 请求取得请求期共享借用，删除请求取得请求期独占借用。
- create/update 继续先更新 TopicConfig 与可选静态映射，再由 BrokerConfig handler 的窄动作执行 coordinator persist 和原 single/increment registration；注册闭包中的 runtime clone 只存在于请求期异步动作。
- Topic validation、system/Mixed 限制、idempotency、DataVersion、POP retry v2/v1/main 删除顺序、config/mapping/offset/inflight/Store 清理、stats/query 响应保持不变。
- 零大小回归证明 Topic handler 不再保活完整 runtime；reviewed baseline 从 328 identities / 899 occurrences 降至 325 / 895，production 从 171/424 降至 169/421，test 从 143/435 降至 142/434，compatibility 保持 14/40。Broker production 从 83/181 降至 81/178，Store 保持 88/243；净删除 2 个 production identity/3 occurrences 与 1 个 test identity/1 occurrence，无 relocation。

## M11-12bc31 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / compatibility / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；Topic handler 3/3 与 phase3/5/6 跨处理器回归 3/3 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker all-feature lib | 612 passed、25 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 动态基线，无 Topic Admin 新失败，因此全套如实记为未通过 |
| Store all-feature lib / RocksDB 专项 | Store 507/507；Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--prune-resolved` 候选与正式最小补丁均从 328/899 精确降至 325/895；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 ArcMut guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、architecture 60/60 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc32 实现

Store auto-switch client construction capability 随 Issue #8473 完成以下边界收敛：

- `AutoSwitchHAClient::new` 的完整 `ArcMut<LocalFileMessageStore>` 参数由 crate-private `from_delegate(DefaultHAClient, broker_id)` 替代，wrapper 不再拥有构造 Store delegate 的能力。
- `AutoSwitchHAService::init` 在原位置显式构造 `DefaultHAClient`，保留 `HAClientError` 到 `HAError::Service` 的映射，再包装并安装客户端；DefaultHAService 初始化、delegate 构造、wrapper 安装的顺序不变。
- `from_delegate` 保留 delegate 报告 broker ID、wrapper 原子 broker ID、master address 与运行状态；两个 client 回归显式构造 delegate，service 的 11 个回归覆盖完整初始化路径。
- reviewed baseline 从 325 identities / 895 occurrences 降至 320 / 890；production 从 169/421 降至 165/417，test 从 142/434 降至 141/433，compatibility 保持 14/40。Store production 从 88/243 降至 84/239，Broker 保持 81/178；净删除 4 个 production identity/4 occurrences 与 1 个 test identity/1 occurrence，4 个保留 test occurrence 经临时 ADR-013 一对一 relocation 审核，无新增 identity。

## M11-12bc32 验证

| 命令 | 结果 |
|---|---|
| Store check / focused / strict Clippy | `cargo check -p rocketmq-store --all-features` 通过；AutoSwitchHAClient 2/2、AutoSwitchHAService 11/11 通过；Store all-target/all-feature strict Clippy 通过 |
| Store / Broker all-feature lib | Store 507/507；Broker 610 passed、27 failed、1 ignored，失败仍集中于既有 lifecycle/Lite/subscription，另有 2 个 controller 固定端口 20011 占用失败；无 HA client/service constructor 新失败，因此 Broker 全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 从 325/895 精确降至 320/890，4 个保留 test occurrence 通过同 item 一对一审核；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 ArcMut guard tests 通过，无新增 identity 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、127/127 guard tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc33 实现

Broker Query Assignment runtime capability 随 Issue #8475 完成以下边界收敛：

- `QueryAssignmentProcessor<MS>` 改为非泛型 `QueryAssignmentProcessor`，删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、MessageStore 泛型和对应 import。
- runtime 构造时只注入启动期 `Arc<BrokerConfig>`、`Arc<MessageStoreConfig>`、可刷新 `TopicRouteInfoManager` 和 `ConsumerAssignmentView`；后者共享 primary consumer table，但只暴露 owned client-id 列表，不泄露可变 `ConsumerGroupInfo`。
- `MessageRequestModeManager` 仍在构造时从原 Store 根路径 load 一次并由 processor clone 共享；NameServer route refresh、route/queue mapping、server-side load-balance 与主 consumer table 实时变化语义保持不变。
- broker name、默认请求模式、默认 POP share queue 数和 server load-balancer 开关仍是启动期快照；动态配置回归证明它们不在运行期 update allowlist，避免能力收窄后虚构动态可见性。
- reviewed baseline 从 320 identities / 890 occurrences 降至 317 / 886；production 从 165/417 降至 163/414，test 从 141/433 降至 140/432，compatibility 保持 14/40。Broker production 从 81/178 降至 79/175，Store 保持 84/239；净删除 2 个 production identity/3 occurrences 与 1 个 test identity/1 occurrence，1 个相邻保留 occurrence 经同 enum item 一对一指纹审核更新，无新增 identity。

## M11-12bc33 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker --all-features` 通过；Query Assignment 10/10、启动期配置 allowlist 1/1、live consumer view 1/1、processor root 1/1、route manager clone 1/1、dispatch 1/1、request-mode sharing 1/1 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker / Store all-feature lib | Broker 614 passed、25 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription/controller 动态基线，无 Query Assignment 新失败，因此 Broker 全套如实记为未通过；Store 507/507 通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式最小补丁从 320/890 精确降至 317/886，1 个相邻保留 occurrence 通过同 enum item 一对一审核；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 ArcMut guard tests 通过，无新增 identity 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、127/127 guard tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| disk recovery | 首次 focused build 因 D 盘空间耗尽失败；按用户授权执行 `cargo clean` 释放 201.2 GiB 后，以上命令在同一源码快照重新执行并取得所列结果 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc34 实现

Store auto-switch single delegate ownership 随 Issue #8478 完成以下边界收敛：

- `AutoSwitchHAService::new` 改为接收 owned `DefaultHAService`，wrapper 删除重复的完整 `ArcMut<LocalFileMessageStore>` field、production import 与构造参数。
- 初始 master/slave 角色、sync-state confirm-offset 更新、HA timeout/alive-replica 查询、epoch/state-machine publication 与角色切换后的 confirm-offset refresh 均经唯一 delegate 的只读 Store 访问。
- `DefaultHAService::create_default_ha_client` 作为 crate-private 窄构造能力复用其 Store owner；AutoSwitch init 继续先初始化 delegate，再按原 `HAClientError` 到 `HAError::Service` 映射包装 client 并安装。
- LocalFileMessageStore production 入口与既有 HA 测试显式先构造 delegate；强引用回归证明 AutoSwitch wrapper 只增加 delegate 所需的一份 Store strong owner，drop 后恢复原计数。
- reviewed baseline 从 317 identities / 886 occurrences 降至 315 / 881；production 从 163/414 降至 161/409，test 保持 140/432，compatibility 保持 14/40。Store production 从 84/239 降至 82/234，Broker 保持 79/175；净删除 2 个 production identity/5 occurrences，18 个保留 occurrence 经同 item 一对一指纹审核更新，无新增 identity。

## M11-12bc34 验证

| 命令 | 结果 |
|---|---|
| Store check / focused / strict Clippy | `cargo check -p rocketmq-store --all-features` 通过；AutoSwitch HA 12/12（含单一 Store owner 回归）与 Default HA 11/11 通过；Store all-target/all-feature strict Clippy 通过 |
| Store / Broker all-feature lib | Store 508/508；Broker 614 passed、25 failed、1 ignored，失败与 bc33 一致，集中于既有 lifecycle/Lite/subscription/controller 动态基线，无 AutoSwitch/Default HA 单元失败，因此 Broker 全套如实记为未通过 |
| Store/RocksDB 专项 | Store/Broker `rocksdb_store` strict Clippy 通过；foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 从 317/886 精确降至 315/881，18 个保留 occurrence 通过同 item 一对一审核；`python scripts/arc_mut_guard.py`、24/24 fixtures 与 67/67 ArcMut guard tests 通过，无新增 identity 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、127/127 guard tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc35 实现

Broker PollingInfo 与 SubscriptionGroup capability 随 Issue #8481 完成以下边界收敛：

- `PollingInfoProcessor<MS>` 改为非泛型 `PollingInfoProcessor`，删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、MessageStore 泛型和对应 import。
- runtime 只注入启动期 `Arc<BrokerConfig>`、共享 `Arc<TopicConfigManager>`、只暴露 find 的 live `SubscriptionGroupConfigLookup` 与 `Weak<dyn PollingCountProvider>`；POP service 已释放时轮询数安全回落为 0，processor 不再直接或间接延长其生命周期。
- `SubscriptionGroupManager` 删除完整 Runtime owner 与 MessageStore 泛型，改持 store path、auto-create、RocksDB 实时 WAL 配置快照和 Store 只读 `StateMachineVersionView`；subscription/forbidden table 与 DataVersion 继续通过共享内部状态保持 clone 可见性。
- Local/Rocks/Generic MessageStore 均返回同一 live 状态机版本只读视图；公开能力不暴露原子写入口，自定义 Store 的默认实现保留构造时 snapshot 兼容行为。
- 请求权限、Topic 校验、SubscriptionGroup 自动创建、JSON/RocksDB persist、DataVersion state-machine version、polling key 与响应码保持不变；弱 provider 回归证明释放后不保活 POP service。
- reviewed baseline 从 315 identities / 881 occurrences 降至 310 / 873；production 从 161/409 降至 157/402，test 从 140/432 降至 139/431，compatibility 保持 14/40。Broker production 从 79/175 降至 75/168，Store 保持 82/234；净删除 4 个 production identity/7 occurrences 与 1 个 test identity/1 occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc35 验证

| 命令 | 结果 |
|---|---|
| Store/Broker check、focused 与 strict Clippy | 两包 check 通过；Store live state view 1/1、PollingInfo 3/3、SubscriptionGroup 默认 20/20（1 忽略）、RocksDB 22/22（1 忽略）通过；两包 all-target/all-feature strict Clippy 通过 |
| Store / Broker broad suites | Store lib 默认 503/503、all-feature 509/509 通过，但共同 integration `file_store_vs_rocksdb_behavior_parity_after_restart` 稳定失败（Local 恢复 2 条、Rocks 0 条），因此全套如实记为未通过；Broker lib 572 passed、24 failed、1 ignored，失败仍为既有 lifecycle/Lite/subscription 动态基线，无 PollingInfo/SubscriptionGroup 新失败 |
| Store/RocksDB 专项 | foundation 82/82、semantics 9/9、Broker rocksdb 21/21、pop_consumer 4/4 通过；失败的 commitlog parity 用例不属于本批次状态版本调用路径，未以专项 PASS 掩盖 broad suite 失败 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 315/881 精确降至 310/873；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc36 实现

Broker TopicQueueMappingClean runtime capability 随 Issue #8483 完成以下边界收敛：

- `TopicQueueMappingCleanService<MS>` 改为非泛型 `TopicQueueMappingCleanService`，删除完整
  `ArcMut<BrokerRuntimeInner<MS>>` field、MessageStore 泛型和对应 import。
- runtime 构造时只注入 broker name、forward timeout、delete window 启动期快照，共享
  `Arc<TopicQueueMappingManager>`、可克隆 `BrokerOuterAPI` 与可选父 TaskGroup；三项配置均不属于运行期更新白名单。
- scheduled scan 继续优先创建在 Broker service TaskGroup 子树；没有显式 ServiceContext 时复用原 ambient Tokio
  fallback。running CAS、固定延迟调度、task snapshot、幂等 start 与有界 shutdown report 语义保持不变。
- expired item 与 old generation 两条扫描路径继续使用同一 live mapping manager，NameServer/remote Broker 查询、
  replacement compare-and-update、持久化和逐 topic yield 顺序保持不变。
- reviewed baseline 从 310 identities / 873 occurrences 降至 303 / 863；production 从 157/402 降至
  155/399，test 从 139/431 降至 134/424，compatibility 保持 14/40。Broker production 从 75/168 降至
  73/165，Store 保持 82/234；净删除 2 个 production identity/3 occurrences 与 5 个 test identity/7
  occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc36 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；TopicQueueMappingClean 7/7 通过，覆盖 owner 源码合同、清理窗口、幂等 start、父 TaskGroup 与健康 shutdown；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 571 passed、26 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 动态基线与一个 controller 固定端口 20011 占用，bc36 的 7 项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 310/873 精确降至 303/863；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；`git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc37 实现

Broker Client heartbeat runtime capability 随 Issue #8485 完成以下边界收敛：

- `ClientManageProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数；保留的
  `MessageStore` 泛型只来自显式 `TransactionTopicRegistration<MS>` Store 兼容边界，不再暴露完整 Broker runtime。
- runtime 构造时注入启动期 `Arc<BrokerConfig>`、共享 `Arc<TopicConfigManager>`、live
  `SubscriptionGroupConfigLookup`、Producer/Consumer registration handle 与 retry-topic registration capability。
- Producer/Consumer handle 只暴露 heartbeat register/unregister 操作，并与主 manager 共享同一 live table；不暴露
  channel selection、housekeeping、配置修改或完整 runtime。
- heartbeat v1/v2 的 retry topic 创建复用显式 registration，保持队列数、读写权限、order、unit sys-flag、
  state-machine version、异步持久化、single/increment NameServer registration 与 master address 更新语义。
- reviewed baseline 从 303 identities / 863 occurrences 降至 300 / 859；production 从 155/399 降至
  153/396，test 从 134/424 降至 133/423，compatibility 保持 14/40。Broker production 从 73/165 降至
  71/162，Store 保持 82/234；净删除 2 个 production identity/3 occurrences 与 1 个 test identity/1
  occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc37 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；ClientManage 7/7 通过，覆盖 production owner 源码合同、property filter、heartbeat v1/v2、Producer/Consumer live registration/unregister 与 retry-topic 参数；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 576 passed、24 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 动态基线，bc37 的 7 项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 303/863 精确降至 300/859；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc38 实现

Broker consumer offset runtime capability 随 Issue #8487 完成以下边界收敛：

- `ConsumerManageProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，改由
  `ConsumerManageProcessorContext` 注入显式 live capability。
- consumer list 使用共享 `ConsumerAssignmentView`，并保留“group 不存在”和“group 存在但 client list 为空”的
  区分；SubscriptionGroup 与 Topic existence 检查继续观察 live manager state。
- 新增 `ConsumerOffsetRequestCapability`，只暴露 commit/query/reset-check 和 legacy query fallback 所需的两项 Store
  只读查询；该 handle 复用 `Arc<ConsumerOffsetManager>` 已有 owner，不新增 Store/ArcMut 强引用。
- static-topic mapping 继续使用 live `TopicQueueMappingManager`，远端 update/query 通过共享 `RpcClientImpl` 和启动期
  `forward_timeout` 转发；逻辑/物理 offset 重写、响应 header、错误码和 remark 保持不变。
- reviewed baseline 从 300 identities / 859 occurrences 降至 298 / 856；production 从 153/396 降至
  151/393，test 保持 133/423，compatibility 保持 14/40。Broker production 从 71/162 降至 69/159，Store
  保持 82/234；净删除 2 个 production identity/3 occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc38 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；production owner 源码合同、live consumer presence/view、offset capability live state/Store-absent fallback 与真实 Update/QueryConsumerOffset round-trip 均通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 576 passed、26 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 动态基线与一次 controller 固定端口 20011 占用，bc38 聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 300/859 精确降至 298/856；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc39 实现

Broker query-message runtime capability 随 Issue #8489 完成以下边界收敛：

- `QueryMessageProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import、构造参数与 clone 传播，
  改为只持默认查询上限和 `QueryMessageStoreCapability<MS>`。
- capability 复用既有 `Weak<EscapeBridge<MS>>` Store provider，仅暴露 Store availability、索引查询和按物理 offset
  读取；请求期才升级 provider，没有把完整 runtime 或 `ArcMut<MS>` 搬入新 wrapper，也不强保活 runtime/Store。
- runtime 初始化时提取 `default_query_max_num` 启动配置并注入 bridge capability；QueryMessage 与 ViewMessageById
  仍路由至同一共享 processor，clone 只复制标量并克隆标准 `Weak` capability。
- Store 缺失时的 `SystemError`/`message store is none`、QueryNotFound、索引安全错误、响应 body 与物理 offset
  查询语义保持不变。
- reviewed baseline 从 298 identities / 856 occurrences 降至 295 / 852；production 从 151/393 降至
  149/390，test 从 133/423 降至 132/422，compatibility 保持 14/40。Broker production 从 69/159 降至
  67/156，Store 保持 82/234；净删除 2 个 production identity/3 occurrences 与 1 个 test identity/1
  occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc39 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；QueryMessage 7/7 与 phase3/phase6 两项真实 processor 路由测试通过，覆盖 owner 源码合同、provider shutdown fail-closed、legacy/typed index 选择、unsafe index remark 与 QueryMessage/ViewMessageById wiring；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 624 passed、25 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 与 controller 收敛动态基线，QueryMessage 聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 298/856 精确降至 295/852；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc40 实现

Broker recall-message runtime capability 随 Issue #8491 完成以下边界收敛：

- `RecallMessageProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import、构造参数与 clone 传播，
  改为 `RecallMessageProcessorContext` 显式组合启动 policy、Topic/Stats handle 和弱 Store capability。
- `RecallMessagePolicy` 只快照当前配置更新白名单之外的 region、recall 开关、接收时间、permission、broker name、
  timer delay 上限和 store host；Broker role 由 `RecallMessageStoreCapability` 在每次请求读取 live 值，保留 controller
  master/slave transition 语义。
- Store capability 只持 `Weak<EscapeBridge<MS>>`，请求期间升级并复用既有 Store 边界执行直接本地 put；不强保活
  runtime/Store，provider 或 Store 缺失时 fail closed，不再以生产 `expect` 处理恢复性生命周期状态。
- Topic existence 与 BrokerStats 更新使用标准共享 handle；RecallMessage 校验顺序、tombstone message properties、
  put-result/response 映射和成功统计保持不变。
- reviewed baseline 从 295 identities / 852 occurrences 降至 292 / 848；production 从 149/390 降至
  147/387，test 从 132/422 降至 131/421，compatibility 保持 14/40。Broker production 从 67/156 降至
  65/153，Store 保持 82/234；净删除 2 个 production identity/3 occurrences 与 1 个 test identity/1
  occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc40 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；RecallMessage 5/5 与 phase3 真实 processor 路由测试通过，覆盖 owner 源码合同、policy 快照、provider shutdown fail-closed、typed error 与 route wiring；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 627 passed、25 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 与 controller 收敛动态基线，RecallMessage 聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 295/852 精确降至 292/848；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc41 实现

Broker end-transaction runtime capability 随 Issue #8493 完成以下边界收敛：

- `EndTransactionProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import、构造参数与 clone
  传播，改为 `EndTransactionProcessorContext` 显式组合启动 policy、共享 BrokerStats handle 和弱 Store capability。
- `EndTransactionPolicy` 只快照当前配置更新白名单之外的 transaction timeout、message size 与 timer 错误响应
  参数；Broker role 由 `EndTransactionStoreCapability` 在每次请求读取 live 值，保留 controller master/slave
  transition 语义。
- Store capability 只持 `Weak<EscapeBridge<MS>>`，请求期间升级并复用既有 Store 边界执行直接本地 put；不强保活
  runtime/Store，provider 或 Store 缺失时返回稳定 `ServiceNotAvailable`，替代原生产 `unwrap` 崩溃路径。
- EndTransaction 的校验顺序、commit/rollback、prepare deletion、put-result/response 映射、BrokerStats 与全局事务
  metrics 语义保持不变。
- reviewed baseline 从 292 identities / 848 occurrences 降至 289 / 844；production 从 147/387 降至
  145/384，test 从 131/421 降至 130/420，compatibility 保持 14/40。Broker production 从 65/153 降至
  63/150，Store 保持 82/234；净删除 2 个 production identity/3 occurrences 与 1 个 test identity/1
  occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc41 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；EndTransaction 7/7 与 phase3 真实 processor 路由测试通过，覆盖 owner 源码合同、policy 快照、provider shutdown fail-closed、错误响应映射与 route wiring；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 631 passed、25 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 与 controller 收敛动态基线，EndTransaction 聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 292/848 精确降至 289/844；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc42 实现

Transaction Store compatibility owner 随 Issue #8495 完成以下边界收敛：

- `TransactionMessageStore` 删除直接 `ArcMut<MS>` field、production import、构造参数与 clone 传播，改为只持
  `Weak<EscapeBridge<MS>>` 的非 owning Store capability。
- `EscapeBridge` 为 transaction half/op read、local put、state-machine version 与 HA master-address 更新提供窄方法；
  `TransactionMessageStore` 不暴露完整 Store，并在 provider/Store 退出后返回无数据、`ServiceNotAvailable` 或跳过更新。
- `TransactionalMessageBridge` 的 escape path 从强 `Arc<EscapeBridge<MS>>` 改为弱 provider；transaction service 与
  retry-topic registration 不再延长完整 runtime/Store 生命周期。
- topic creation 必须取得 live state-machine version，provider 不可用时直接停止创建，避免用伪造版本继续推进；
  transaction read/put/topic registration/master-address/escape 的正常存活期语义保持不变。
- reviewed baseline 从 289 identities / 844 occurrences 降至 287 / 841；production 从 145/384 降至
  143/381，test 保持 130/420，compatibility 保持 14/40。Broker production 从 63/150 降至 61/147，Store
  保持 82/234；净删除 2 个 production identity/3 occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc42 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；transaction queue 18/18 与 phase3 真实 processor 路由测试通过，覆盖 weak provider source contract、provider shutdown fail-closed、transaction ownership contract 与 route wiring；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 634 passed、24 failed、1 ignored；失败仍集中于既有 lifecycle/Lite/subscription 基线，bc41 中动态失败的 controller convergence 用例本轮通过，transaction 聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 289/844 精确降至 287/841；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc43 实现

Broker PeekMessage runtime capability 随 Issue #8497 完成以下边界收敛：

- `PeekMessageProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，改为
  `PeekMessageProcessorContext` 显式组合最小请求能力。
- `PeekMessagePolicy` 只快照 broker permission/IP、revive queue 数、heap transfer 与 retry-topic v2；TopicConfig、
  SubscriptionGroup、BrokerStats 继续使用共享 live handle。
- consumer offset 查询改用弱 `ConsumerOffsetQueryCapability`；Store 的 now/min/max/get-message 改用请求期升级的弱
  `EscapeBridge` provider，POP buffer offset 改用弱 `PopBufferMergeService` provider，不强保活 manager/runtime/Store。
- provider 退出后按原 Store/processor 缺失语义返回 0、-1 或无消息；延迟计算使用饱和减法避免 provider 在请求中退出时
  发生下溢。权限、重试主题、位点校正、消息读取、rest-num 与统计语义保持不变。
- reviewed baseline 从 287 identities / 841 occurrences 降至 285 / 838；production 从 143/381 降至
  141/378，test 保持 130/420，compatibility 保持 14/40。Broker production 从 61/147 降至 59/144，Store
  保持 82/234；净删除 2 个 production identity/3 occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc43 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；Peek/offset capability 5/5 与 phase3 真实 processor 路由测试通过，覆盖 policy 快照、weak provider shutdown fail-closed、offset live/exit 与 source contract；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 639 passed、24 failed、1 ignored；24 项与 bc42 相同，仍集中于既有 lifecycle/Lite/subscription 基线，Peek 聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 287/841 精确降至 285/838；正式最小 baseline 补丁后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc44 实现

Broker Notification/POP long-polling runtime capability 随 Issue #8499 完成以下边界收敛：

- `NotificationProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，改由
  `NotificationProcessorContext` 注入权限/重试 policy、Topic/Subscription/Order 查询和弱 offset/Store/POP provider。
- `PopLongPollingService` 删除 MessageStore 泛型与完整 runtime owner，改由 `PopLongPollingServiceContext` 注入容量
  policy、Topic/Subscription 查询和可选父 `TaskGroup`；扫描与 wake-up 任务继续由 owned child TaskGroup 取消和等待。
- Store min/max 与 POP buffer offset 在请求期升级弱 provider；消费位点查询不强保活带 Store 的 manager。provider 退出后
  按原缺失语义返回 0、-1 或无消息，不发生 panic，也不延长 runtime/Store/POP 生命周期。
- 权限、retry topic v1/v2、order block、消息可用性、polling 容量、超时、资源清理、通知与 wake-up 语义保持不变；
  `ConsumerOrderInfoManager` 改由标准 `Arc` 共享，不包含完整 runtime/Store owner。
- reviewed baseline 从 285 identities / 838 occurrences 降至 281 / 832；production 从 141/378 降至
  137/372，test 保持 130/420，compatibility 保持 14/40。Broker production 从 59/144 降至 55/138，Store
  保持 82/234；净删除 4 个 production identity/6 occurrences。1 个相邻保留 occurrence 经 ADR-013 一对一
  relocation 审核，无新增 identity 或临时 approval 提交。

## M11-12bc44 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；Notification 5/5、POP long-polling 生命周期 3/3 与 phase3 真实 processor 路由 1/1 通过，覆盖 policy、weak provider shutdown、weak back-reference、父 TaskGroup、active scan owner release 与 source contract；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 643 passed、24 failed、1 ignored；24 项与 bc43 相同，仍集中于既有 lifecycle/Lite/subscription 基线；另一次运行动态出现既有 controller convergence 失败，聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 从 285/838 精确降至 281/832；1 个相邻保留 occurrence 经临时 ADR-013 一对一 relocation 审核且 approval 不提交；正式最小 baseline 补丁后 guard、candidate compare、24/24 fixtures 与 67/67 tests 通过，无新增 identity |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc45 实现

Broker ChangeInvisibleTime runtime capability 随 Issue #8501 完成以下边界收敛：

- `ChangeInvisibleTimeProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，
  并删除会间接强保活 runtime 的 `Arc<PopMessageProcessor<MS>>` field，改由显式 context 组合最小请求能力。
- `ChangeInvisibleTimePolicy` 只快照 revive topic、store host 与 POP log 开关；Topic 与 BrokerStats 使用标准共享 handle，
  consumer offset 复用 weak query capability，queue lock 使用不包含 Broker runtime 的独立可克隆 manager。
- Store min/max 与 specific-queue put 改为请求期升级的弱 `EscapeBridge` provider；POP buffer ack 与 order-info update
  分别改为弱 `PopBufferMergeService`/`ConsumerOrderInfoManager` provider，不延长 runtime/Store/POP 生命周期。
- provider 存活时普通 POP/顺序 POP 的 offset 校验、checkpoint/ack、统计、锁、延时和响应语义保持不变；provider
  退出时返回 `ServiceNotAvailable` 或按既有 ack fallback fail closed，不发生 panic。
- reviewed baseline 从 281 identities / 832 occurrences 降至 279 / 829；production 从 137/372 降至
  135/369，test 保持 130/420，compatibility 保持 14/40。Broker production 从 55/138 降至 53/135，Store
  保持 82/234；净删除 2 个 production identity/3 occurrences。1 个保留的外层 processor wrapper 经 ADR-013
  一对一 relocation 审核，无新增 identity 或临时 approval 提交。

## M11-12bc45 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；ChangeInvisibleTime capability 3/3 与 phase3 真实 processor 路由 1/1 通过，覆盖 policy 快照、weak Store/POP/order provider shutdown fail-closed 与 source contract；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 645 passed、25 failed、1 ignored；其中 24 项与 bc44 相同，仍集中于既有 lifecycle/Lite/subscription 基线；额外的既有 controller convergence 动态失败单独复跑 1/1 通过，bc45 聚焦项均通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 从 281/832 精确降至 279/829；1 个保留的外层 processor wrapper 经临时 ADR-013 一对一 relocation 审核且 approval 不提交；正式最小 baseline 补丁后 guard、candidate compare、24/24 fixtures 与 67/67 tests 通过，无新增 identity |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc46 实现

Broker POP Lite long-polling runtime capability 随 Issue #8503 完成以下边界收敛：

- `PopLiteLongPollingService` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，并移除
  `MessageStore` 泛型；service 不再直接或间接保活完整 runtime/Store。
- `PopLiteLongPollingPolicy` 只快照 polling map 容量、全局 polling 上限和单客户端上限；
  `PopLiteLongPollingServiceContext` 只组合 policy、可克隆 `LiteEventDispatcher` 与可选父 `TaskGroup`。
- `PopLiteMessageProcessor` 仅在组合根从 runtime 提取上述能力；long-polling 的 scan/wakeup child task 明确挂在 Broker
  service TaskGroup 下，缺少注入 context 时继续使用既有 ambient Tokio fallback。
- polling map 容量、全局/客户端限流、过期扫描、事件 wake-up、幂等启动、并发 start/shutdown 串行化和有界 shutdown
  语义保持不变；processor 的其余请求能力仍由后续独立切片收窄。
- reviewed baseline 从 279 identities / 829 occurrences 降至 277 / 826；production 从 135/369 降至
  133/366，test 保持 130/420，compatibility 保持 14/40。Broker production 从 53/135 降至 51/132，Store
  保持 82/234；净删除 2 个 production identity/3 occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc46 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；POP Lite long-polling capability 6/6 与 phase3 真实 processor 路由 1/1 通过，覆盖 policy 快照、source contract、weak processor back-reference、父 TaskGroup、active scan owner release 与 start/shutdown/restart 串行化；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 649 passed、24 failed、1 ignored；24 项与 bc45 登记基线相同，仍集中于既有 lifecycle/Lite/subscription 行为基线；bc46 聚焦项全部通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 279/829 精确降至 277/826；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc47 实现

Broker POP Lite message processor runtime capability 随 Issue #8505 完成以下边界收敛：

- `PopLiteMessageProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，改由
  `PopLiteMessageProcessorContext` 显式组合请求和生命周期能力。
- `PopLiteMessagePolicy` 只快照 broker IP/permission、默认 event 上限与 full-dispatch delay；TopicConfig 使用共享
  manager，SubscriptionGroup 使用只读 lookup，Lite event dispatcher 保留可克隆 live handle。
- consumer offset query/reset/commit 改为请求期升级 `Weak<ConsumerOffsetManager<MS>>`；Store get/max-offset 改为请求期
  升级 `Weak<EscapeBridge<MS>>`，并通过可失败的 Store borrow 保留 LiteLifecycle 的 LMQ max-offset 语义。
- queue lock 和 long-polling service 均显式继承 Broker service TaskGroup；provider 退出后按无消息、offset 缺失或 no-op
  commit fail closed，不 panic，也不强保活 runtime/Store。
- 校验顺序、Topic/Subscription 权限、LMQ 读取与 offset 校正、顺序消费、event requeue/full dispatch 和 polling 响应
  语义保持不变；外层组合逻辑集中在 Broker runtime initialization root。
- reviewed baseline 从 277 identities / 826 occurrences 降至 273 / 819；production 从 133/366 降至
  131/361，test 从 130/420 降至 128/418，compatibility 保持 14/40。Broker production 从 51/132 降至
  49/127，Store 保持 82/234；净删除 2 个 production identity/5 occurrences 与 2 个 test identity/2
  occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc47 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；POP Lite processor/context/provider/lifecycle 8/8 与 phase3 真实 processor 路由 1/1 通过，覆盖 policy、source contract、weak provider shutdown、weak processor back-reference、父 TaskGroup、active scan owner release 与 start/shutdown/restart 串行化；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 651 passed、25 failed、1 ignored；其中 24 项与 bc46 登记基线相同，额外的既有 controller failover convergence 超时单独串行复跑 1/1 通过；bc47 聚焦项全部通过，因此全套如实记为未通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 277/826 精确降至 273/819；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc48 实现

Broker Lite subscription control runtime capability 随 Issue #8507 完成以下边界收敛：

- `LiteSubscriptionCtlProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，改由
  `LiteSubscriptionCtlContext` 显式组合容量、注册、查询、Store 与 order-info 能力。
- `LiteSubscriptionCtlPolicy` 只快照全局 lite subscription 上限；registry、event dispatcher 与 SubscriptionGroupManager
  使用共享 live view，保留 channel、group attribute 与 active-subscription 实时语义。
- consumer offset query/assign-reset 与 Store LMQ max-offset 复用弱 POP Lite capability；order-info clear 使用
  `Weak<PopLiteMessageProcessor<MS>>`，provider 退出后分别按 -1、false、0 或 no-op fail closed。
- partial/complete add/remove、stale version、exclusive conflict、quota、offset policy/offset/tail-N 与 unsubscribe reset
  语义保持不变；测试 seed helper 改为普通 BrokerRuntime 借用并删除 glob/ArcMut 传播。
- `LiteManager` 与 `LiteSubscriptionCtl` 的无状态外层 wrapper 改为标准 Arc；六个 LiteManager request code 共享一个
  processor，删除重复 ArcMut wrapper 而不改变 handler 内层 runtime capability（后者仍计入后续债务）。
- reviewed baseline 从 273 identities / 819 occurrences 降至 269 / 805；production 从 131/361 降至
  129/349，test 从 128/418 降至 126/416，compatibility 保持 14/40。Broker production 从 49/127 降至
  47/115，Store 保持 82/234；净删除 2 个 production identity/12 occurrences 与 2 个 test identity/2
  occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc48 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；LiteSubscription policy/source/weak-provider 3/3 与 phase3 真实 processor 路由 1/1 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 654 passed、25 failed、1 ignored；其中 24 项与 bc47 登记基线相同，额外的既有 controller failover convergence 用例单独串行复跑仍在同一 slave-view 等待点超时；bc48 聚焦项全部通过且不触及 controller/NameServer/HA 路径，因此全套与单项均如实记为未通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 273/819 精确降至 269/805；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc49 实现

Broker Lite manager runtime capability 随 Issue #8509 完成以下边界收敛：

- `LiteManagerProcessor` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import 与构造参数，改由
  `LiteManagerContext` 显式组合启动 policy、metadata view 与弱查询 capability。
- `LiteManagerPolicy` 只快照 store type、LMQ 上限、broker name 与 full-dispatch limit/delay；TopicConfigManager、
  SubscriptionGroupManager、LiteSubscriptionRegistry、LiteEventDispatcher 与 LiteLifecycleManager 保持共享 live view。
- `LiteShardingView` 只共享 publish-route table 与当前 broker name；Lite lag calculator 改为消费
  `LiteConsumerLagDataSource`，两者均删除完整 runtime 参数。
- consumer offset snapshot/query/size、Store queue stats/offset/timestamp 与 POP Lite order-info 通过 `Weak` provider 查询；
  provider 退出后按空表、-1、0、false 或 None fail closed，不 panic、不新增 runtime/Store 强保活。
- GetBrokerLiteInfo、GetParentTopicInfo、GetLiteTopicInfo、GetLiteClientInfo、GetLiteGroupInfo 与 TriggerLiteDispatch 的
  校验、metadata、sharding、lag/offset 与 dispatch 语义保持不变；六路由继续共享单一标准 Arc processor。
- reviewed baseline 从 269 identities / 805 occurrences 降至 267 / 802；production 从 129/349 降至
  127/346，test 保持 126/416，compatibility 保持 14/40。Broker production 从 47/115 降至 45/112，Store
  保持 82/234；净删除 2 个 production identities/3 occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc49 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；LiteManager policy/source/weak-provider 3/3 与 phase3 真实 processor 路由 1/1 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 657 passed、25 failed、1 ignored；失败集合与 bc48 完全相同（24 项既有 lifecycle/Lite/subscription 基线，加同一 controller slave-view convergence 超时），新增 3 项 bc49 回归全部通过，未新增失败；因此全套如实记为未通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 269/805 精确降至 267/802；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc50 实现

Broker slave metadata synchronization capability 随 Issue #8511 完成以下边界收敛：

- `SlaveSynchronize` 删除完整 `ArcMut<BrokerRuntimeInner<MS>>` field、production import、构造参数与
  subscription-group `mut_from_ref`，改由 `SlaveSynchronizeContext` 显式组合所需能力。
- `SlaveSynchronizePolicy` 只快照 broker address 与 timer-wheel 开关；BrokerOuterAPI 保持共享客户端能力。
- TopicConfigManager、TopicConfigCoordinator、TopicQueueMappingManager、ScheduleMessageService 与 Timer Store 改为
  `Weak` provider；ConsumerOffsetManager 在 MessageStore 发布后晚绑定为 Weak，避免破坏启动期 `Arc::get_mut` 唯一性。
- MessageRequestModeManager 在 QueryAssignment processor 构造后晚绑定；SubscriptionGroupManager 使用可释放共享槽。
  shutdown 在 metadata/Store detach 前清空 subscription/request-mode 强 capability，避免 RocksDB owner 被泄漏 carrier 延寿。
- topic config/mapping、consumer offset、delay offset、subscription group、message request mode、timer checkpoint/metrics 的
  同步顺序和版本语义保持不变；provider 尚未绑定或已退出时记录告警并 fail closed。
- reviewed baseline 从 267 identities / 802 occurrences 降至 264 / 798；production 从 127/346 降至
  124/342，test 保持 126/416，compatibility 保持 14/40。Broker production 从 45/112 降至 42/108，Store
  保持 82/234；净删除 3 个 production identities/4 occurrences，无 relocation、新增 identity 或临时 approval。

## M11-12bc50 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；SlaveSynchronize policy/source/late-binding/weak-provider 4/4 与 phase3 真实 processor 路由 1/1 通过；两项 RocksDB metadata migration/recovery 2/2 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 661 passed、25 failed、1 ignored；失败集合与 bc49 完全相同（24 项既有 lifecycle/Lite/subscription 基线，加同一 controller slave-view convergence 超时），新增 4 项 bc50 回归全部通过，未新增失败；因此全套如实记为未通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 267/802 精确降至 264/798；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；最终补丁后复跑 `git diff --check`；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc51 实现

Broker mut-from-ref lint boundary 随 Issue #8513 完成以下收口：

- 删除 `rocketmq-broker/src/lib.rs` 的 crate-wide `#![allow(clippy::mut_from_ref)]`。
- Broker 与 root workspace all-target/all-feature strict Clippy 在没有该豁免时通过；后续任何新的共享引用可变逃逸实现会由
  Clippy 默认拒绝，确需兼容时只能使用带理由的最窄 item-level allowance。
- reviewed baseline 从 264 identities / 798 occurrences 降至 263 / 797；production 从 124/342 降至
  123/341，test 保持 126/416，compatibility 保持 14/40。Broker production 从 42/108 降至 41/107，Store
  保持 82/234；净删除 1 个 production identity/1 occurrence，无 relocation、新增 identity 或临时 approval。

## M11-12bc51 验证

| 命令 | 结果 |
|---|---|
| strict Clippy | Broker all-target/all-feature 与 root workspace all-target/all-feature strict Clippy 均在无 crate-wide `mut_from_ref` allowance 时通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 264/798 精确降至 263/797；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check`、workspace strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc52 实现

Broker processor registry wrapper 随 Issue #8517 完成以下收口：

- AckMessageProcessor 与 ChangeInvisibleTimeProcessor 增加共享请求入口，所有请求期 helper 改为普通共享借用；
  BrokerProcessorType 与 BrokerRuntime 的 wrapper/handle 使用标准 Arc。
- Ack 的 start、role-status 与 shutdown 只操作 PopReviveService 已有 atomic flag 与受锁 TaskGroup；这些生命周期 API
  改为共享借用，不再要求外层 processor ArcMut。
- AdminBrokerProcessor 保留真实 `&mut self` 配置语义，但 registry 改为 `Arc<tokio::sync::Mutex<_>>`，由显式 async
  mutex 串行化低频管理请求，避免 concurrent `&mut`，也不阻塞 Broker 数据面 processor。
- reviewed baseline 从 263 identities / 797 occurrences 降至 260 / 787；production 从 123/341 降至
  121/332，test 从 126/416 降至 125/415，compatibility 保持 14/40。Broker production 从 41/107 降至
  39/98，Store 保持 82/234；净删除 2 个 production identities/9 occurrences 与 1 个 test identity/1 occurrence，
  无 relocation、新增 identity 或临时 approval。

## M11-12bc52 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused routes | `cargo check -p rocketmq-broker` 通过；standard-ownership、ChangeInvisible weak capability、Admin leaf ownership、phase3 production routes 与 phase5 Admin fallback 共 5/5 通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 263/797 精确降至 260/787；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| Broker broad lib suites | default suite 616 passed、25 failed、1 ignored；all-feature suite 660 passed、26 failed、1 ignored，较已登记 25 项仅多一次 `blocking_shutdown_hook_cannot_extend_the_absolute_deadline` 时序失败，隔离立即复跑 1/1 通过；变更涉及的 ownership/routes 测试均通过，无新增可复现失败 |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc53 实现

Ack 内部 capability 随 Issue #8519 完成以下收口：

- `AckMessageProcessor` 删除完整 `BrokerRuntimeInner` 与强 `PopMessageProcessor` owner，组合根显式注入启动期
  revive topic/store host policy、共享 TopicConfig/PopInflight 与弱 ConsumerOffset/ConsumerOrder/EscapeBridge/POP capability；Ack 源文件不再导入或使用
  `ArcMut`，Store/order provider 退出时 fail closed，不再通过 Store `unwrap` 触发恢复性 panic。
- PopRevive service 由 Broker composition root 构造，Ack 只持 `Vec<Arc<PopReviveService<_>>>`；start、
  merge-and-revive 与 revive-from-checkpoint task receiver 改为标准 Arc。并发更新的 revive timestamp 改为
  `AtomicI64`，retry-topic helper 收窄为共享借用，既有 TaskGroup/cancellation/shutdown 顺序保持不变。
- PopInflight counter 增加共享 Clone handle，克隆只共享既有原子与 DashMap 状态；Ack 不再为 decrement 访问完整
  runtime。新增 policy 快照、弱 provider 退出和 clone counter 共享更新测试。
- reviewed baseline 从 260 identities / 787 occurrences 降至 257 / 779；production 从 121/332 降至
  118/324，test 保持 125/415，compatibility 保持 14/40。Broker production 从 39/98 降至 36/90，Store
  保持 82/234；净删除 3 个 production identities/8 occurrences，其中 Ack owner 5 occurrences、PopRevive
  task receiver 3 occurrences，无 relocation、新增 identity 或临时 approval。
- 执行清单 R02 完成；31 项最小审查清单已完成 1 项、剩余 30 项。正式进度仍为 75/82，PR-M11-12 未提前关闭。

## M11-12bc53 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；Ack policy/weak capability、共享 inflight counter 与两个 PopRevive 并发/可见性测试共 5/5 通过；Broker all-target/all-feature strict Clippy 通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 260/787 精确降至 257/779；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过，无 relocation、新增 identity 或临时 approval |
| Broker broad lib suites | default suite 未通过：618 passed、26 failed、1 ignored；相对已登记 25 项多出的固定端口 controller 并行冲突与同组 controller 用例均隔离复跑 1/1 通过。all-feature suite 未通过：665 passed、24 failed、1 ignored，失败少于已登记基线；新增 2 个 Ack 测试与全部聚焦项均通过，无新增可复现失败 |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；`git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc54 实现

Broker pre-online capability 随 Issue #8521 完成以下收口：

- `BrokerPreOnlineService` 删除完整 `BrokerRuntimeInner` owner，改持不可变 `BrokerPreOnlinePolicy`、实时
  `BrokerOnlineRoleState`、弱 Store/HA 与 metadata provider、registration capability 和显式 special-service capability。
- controller 角色切换与配置替换同步发布 live broker ID、min broker ID/address 和 isolated 状态；registration 使用实时
  broker ID，Store/HA 只通过弱 `EscapeBridge` 窄接口访问，所有 provider 退出时 fail closed。
- pre-online service 延迟到 Store、Timer、transaction、Ack 等 provider 就绪后构造，后台循环挂载 Broker 父
  `TaskGroup`；shutdown 提前到 Store/metadata detach 之前且不再通过 unwrap/panic 处理恢复性停止失败。
- transaction check service slot 改为标准 Arc 以提供弱 special-service capability；旧模块保留薄 re-export，既有公开路径
  不变。新增 live-role、Store fail-closed 和 source-contract 测试。
- reviewed baseline 从 257 identities / 779 occurrences 降至 255 / 775；production 从 118/324 降至
  116/320，test 保持 125/415，compatibility 保持 14/40。Broker production 从 36/90 降至 34/86，Store
  保持 82/234；pre-online owner 净删除 2 identities/3 occurrences，无调用方的 runtime start helper 额外删除
  1 occurrence。3 个保留 occurrence 完成一对一指纹审核，无新增 identity 或临时 approval。
- 执行清单 R08 完成；31 项最小审查清单已完成 2 项、剩余 29 项。正式进度仍为 75/82，PR-M11-12 未提前关闭。

## M11-12bc54 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；pre-online capability 6/6、父 TaskGroup 1/1、role persistence 1/1、Store 主从切换 2/2 通过；Broker all-target/all-feature strict Clippy 通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 257/779 精确降至 255/775；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过；3 个保留 occurrence 完成一对一指纹审核，无新增 identity 或临时 approval |
| Broker broad lib suites | default suite 未通过：622 passed、25 failed、1 ignored；其中固定端口 controller 并行冲突隔离复跑 1/1 通过，其余属于已登记 Lite/生命周期失败集。all-feature suite 未通过：668 passed、24 failed、1 ignored，失败集未扩张；新增 3 个 capability 测试与全部聚焦项均通过，无新增可复现失败 |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；`git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc55 实现

Broker send/reply capability 随 Issue #8523 完成以下收口：

- `SendMessageProcessor`、`ReplyMessageProcessor` 与共享 `Inner` 删除完整 `BrokerRuntimeInner` owner、模块 import、
  构造传播和 hook `ArcMut`，改为标准 `Arc` 与不可变 hook 集合。
- 新增基于 `ArcSwap` 的 `SendMessagePolicyState`，Broker/Store 配置、Store host 和 controller broker ID/role
  变化时发布完整不可变代际；请求路径不持有配置锁或完整 runtime。
- Store 访问改为只持标准 `Weak<EscapeBridge>` 的 capability。send single/batch append 保留 typed Store error，
  provider 缺失时返回 Append/NotStarted；health、progress、look/put 与 reply Store 缺失均 fail closed。
- Topic 查询、创建、持久化、single/incremental registration、static-topic mapping 和 master address/order config 更新
  改由显式 capability 完成，旧 send-topic runtime 宏删除。Subscription/Rebalance/Stats 与 producer reply channel
  也改为窄 handle；reply 保持先 push client、再按配置写 Store 的顺序。
- reviewed baseline 从 255 identities / 775 occurrences 降至 247 / 760；production 从 116/320 降至
  110/307，test 从 125/415 降至 123/413，compatibility 保持 14/40。Broker production 从 34/86 降至
  28/73，Store 保持 82/234；净删除 6 个 production identities/13 occurrences 与 2 个 test identities/2
  occurrences。1 个保留 BrokerRuntime root constructor 经临时 ADR-013 一对一 relocation 审核，无新增 identity
  或提交态临时 approval。
- 执行清单 R03 完成；31 项最小审查清单已完成 3 项、剩余 28 项。正式进度仍为 75/82，PR-M11-12 未提前关闭。

## M11-12bc55 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；send/message-builder/policy/source-contract 27/27、reply 9/9、consumer-send-back 1/1、producer reply registry 1/1 通过；Broker all-target/all-feature strict Clippy 通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 255/775 精确降至 247/760；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过；1 个保留 BrokerRuntime root constructor 完成临时 ADR-013 一对一 relocation 审核，无新增 identity 或提交态临时 approval |
| Broker broad lib suites | default suite 未通过：625 passed、25 failed、1 ignored，失败仍为既有 lifecycle/Lite 集合与固定端口 controller 瞬态，controller 隔离复跑 1/1 通过。all-feature suite 原始结果 668 passed、27 failed、1 ignored；多出的 deadline/controller 三项均隔离复跑 1/1 通过，因此可复现失败集保持既有 24 项，新增 3 个测试与全部聚焦项均通过 |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures。额外 error architecture guard 如实发现未改动文件中的既有 4 条基线 finding：Client retry token 2 条、Schedule source stringification 2 条 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；`git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc56 实现

Broker pull capability 随 Issue #8525 完成以下收口：

- `PullMessageProcessor` 与 `DefaultPullMessageResultHandler` 删除完整 `BrokerRuntimeInner` owner、`ArcMut`、
  `use super::*` 测试依赖和构造传播，改为共享 `Arc<PullMessageProcessorContext>`。
- 新增基于 `ArcSwap` 的 `PullMessagePolicyState`。Broker/Store 配置与 controller broker ID/role 变化时发布完整
  不可变代际；请求路径不持有配置锁或完整 runtime。
- RPC、consumer、filter、subscription、topic、queue mapping、offset、broadcast offset、stats、live role、
  cold-data 与 long-polling 由显式 capability 组合；long-polling 服务在 runtime 初始化阶段一次安装。
- Store 访问只持标准 `Weak<EscapeBridge>`，并通过 `EscapeBridge` 的本地读取、offset/time、cold-area
  provider 方法访问底层 Store；provider 不可用时返回系统错误并 fail closed，不再依赖 `unwrap` 隐含生命周期。
- reviewed baseline 从 247 identities / 760 occurrences 降至 241 / 749；production 从 110/307 降至
  106/300，test 从 123/413 降至 121/409，compatibility 保持 14/40。Broker production 从 28/73 降至
  24/66，Store 保持 82/234；净删除 4 个 production identities/7 occurrences 与 2 个 test identities/4
  occurrences。1 个保留 BrokerRuntime root constructor 经临时 ADR-013 一对一 relocation 审核，无新增 identity
  或提交态临时 approval。
- 执行清单 R05 完成；31 项最小审查清单已完成 4 项、剩余 27 项。正式进度仍为 75/82，PR-M11-12 未提前关闭。

## M11-12bc56 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；pull capability/policy/provider/source-contract 与请求处理聚焦测试 18/18 通过；Broker all-target/all-feature strict Clippy 通过 |
| reviewed baseline / fixtures | 正式最小 baseline 补丁从 247/760 精确降至 241/749；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过；1 个保留 BrokerRuntime root constructor 完成临时 ADR-013 一对一 relocation 审核，无新增 identity 或提交态临时 approval |
| Broker broad lib suites | default suite 未通过：628 passed、24 failed、1 ignored；all-feature suite 原始结果 672 passed、25 failed、1 ignored，多出的 absolute-deadline 瞬态隔离复跑 1/1 通过，因此可复现失败集保持既有 lifecycle/Lite/subscription 24 项。新增 pull 测试与全部聚焦项均通过 |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/target/baseline、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures。额外 error architecture guard 如实发现未改动文件中的既有 4 条基线 finding：Client retry token 2 条、Schedule source stringification 2 条 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；`git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc57 实现

Broker admin config boundary 随 Issue #8527 完成以下收口：

- `AdminBrokerProcessor` 与 `BrokerConfigRequestHandler` 删除直接 `ArcMut<BrokerRuntimeInner>` import、constructor
  参数和字段类型；admin config 更新与 commit-log read mode 不再从共享引用调用 `mut_from_ref`。
- 新增 `BrokerAdminRuntimeHandle`，将唯一 compatibility owner 明确归并到 R01 组合根；Admin processor 的 Tokio
  mutex 继续串行化请求，共享与独占 runtime 借用在 handle 上显式区分。
- Topic persist/register 与 controller role-change 的异步动作通过 handle 的窄委托保持原执行顺序；其他已经完成
  leaf 化的 Admin handler 继续只接收请求期借用，不重新长期保活 runtime root。
- admin processor 测试删除 `use super::*`，并增加 source contract，防止两个目标文件重新引入直接 ArcMut 或
  `mut_from_ref`。
- 原 R06 5 个 production identities/7 occurrences 与 1 个 test identity/1 occurrence 全部删除，其中 dispatcher
  1/1 owner 经临时 ADR-013 一对一迁移为 R01 carrier。reviewed baseline 从 241/749 净降至 236/742；production
  从 106/300 净降至 102/294，test 从 121/409 降至 120/408，compatibility 保持 14/40；Broker production 从
  24/66 降至 20/60，Store 保持 82/234。R01 精确余量由 7/34 调整为 8/35，临时 approval 不提交。
- 执行清单 R06 完成；31 项最小审查清单已完成 5 项、剩余 26 项。正式进度仍为 75/82，PR-M11-12 未提前关闭。

## M11-12bc57 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；admin config default 8/8、all-feature/RocksDB 9/9 通过；Broker all-target/all-feature strict Clippy 通过 |
| Admin broad focused suite | 53 passed、1 failed；唯一失败为已登记的 Lite subscription-group offset cleanup 基线，config、Topic、controller role-change、source contract 与其他 request-borrow 路径均通过 |
| reviewed baseline / fixtures | ADR-013 identity relocation 将 admin dispatcher 1/1 owner 移到 R01 composition-root handle；正式最小 baseline 从 241/749 净降至 236/742，正常 guard 与 reviewed candidate compare 通过，无扩大的 governed debt 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、24/24 ArcMut fixtures、67/67 guard tests、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures。额外 error architecture guard 如实发现未改动文件中的既有 4 条基线 finding：Client retry token 2 条、Schedule source stringification 2 条 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；`git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc58 实现

Broker offset/failover capability 随 Issue #8529 完成以下收口：

- `ConsumerOffsetManager` 删除直接 Store ArcMut 字段、constructor 参数与 setter，改为一次绑定的弱
  `EscapeBridge` provider；请求 fallback、state-version 与未订阅 Topic 清理只调用所需 Store 查询。
- `EscapeBridge` 删除完整 `BrokerRuntimeInner` owner，改持 `EscapeBridgePolicyState`、共享
  `TopicRouteInfoManager`、`BrokerOuterAPI` 与晚绑定 `EscapeBridgeStoreCapability`。
- failover policy 以 `ArcSwap` 发布 broker name/id、acting-master、remote-escape 与 Store role 的不可变代际；普通
  broker config 更新、Store config 更新及 controller role-change 都同步发布，不把配置锁带入消息路径。
- 唯一遗留 Store 指针压缩到 `LegacyEscapeStoreOwner` 的单字段兼容边界；BrokerRuntime 在 Store 初始化或测试替换
  时显式绑定。offset/failover 两个目标文件均不再出现 ArcMut、完整 runtime 或 `mut_from_ref`，测试 glob 同步删除。
- 原 R07 4 个 production identities/8 occurrences 与 1 个 test identity/1 occurrence 全部删除，其中原 failover
  Store owner 经临时 ADR-013 从 2 occurrences 压缩并一对一迁移为 R01 的 1/1 owner。reviewed baseline 从
  236/742 净降至 232/734；production 从 102/294 降至 99/287，test 从 120/408 降至 119/407，compatibility
  保持 14/40；Broker production 从 20/60 降至 17/53，Store 保持 82/234。R01 精确余量由 8/35 调整为 9/36。
- 执行清单 R07 完成；31 项最小审查清单已完成 6 项、剩余 25 项。正式进度仍为 75/82，PR-M11-12 未提前关闭。

## M11-12bc58 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | `cargo check -p rocketmq-broker` 通过；default failover 7/7、offset 15/15 通过；all-feature failover 7/7、offset/RocksDB 17/17 通过；Broker all-target/all-feature strict Clippy 通过 |
| BrokerRuntime composition suite | 60 passed、19 failed；Store 写入、角色切换、processor 接线等本切片目标路径通过；失败集均属于已登记的 Lite 状态基线、lifecycle 基线或并行固定端口 controller 瞬态 |
| reviewed baseline / fixtures | ADR-013 将原 failover Store owner 从 2 occurrences 压缩并迁移到 R01 的 1/1 compatibility owner，同时审核同一 BrokerRuntime Store constructor 指纹迁移；正式 baseline 从 236/742 降至 232/734，正常 guard 与 reviewed candidate compare 通过，无扩大的 governed debt 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、24/24 ArcMut fixtures、67/67 guard tests、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures。error architecture guard 延长只读扫描后仍仅发现未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc59 实现

Broker POP capability 随 Issue #8531 完成以下收口：

- `PopMessageProcessor`、`PopBufferMergeService` 与 `PopReviveService` 删除完整 `BrokerRuntimeInner`、ArcMut
  与 `mut_from_ref`，分别改持 `PopMessageProcessorContext`、`PopBufferMergeContext`、`PopReviveContext`。
- `PopPolicyState` 以 `ArcSwap` 发布 broker/store/POP/rocks/timer 配置的不可变代际；普通 broker config、Store
  config、Store host 与 controller role-change 更新都同步发布，不把配置锁带入消息处理路径。
- Topic、subscription、filter、consumer、offset、order、stats 与 inflight 使用显式 capability；Store 经弱
  `EscapeBridge` provider fail closed，buffer/revive background work 继续由父 `TaskGroup` 拥有并在 detach 前停止。
- POP 请求聚合删除 `ArcMut<GetMessageResult>`，改为栈上独占值和串行 `&mut GetMessageResult` 借用；不存在跨任务
  共享可变结果，也不为兼容旧签名新增锁。
- 原 R04 的 8 个 production identities/17 occurrences 与 3 个 test identities/3 occurrences 全部删除，无
  relocation、新增 identity 或临时 approval。reviewed baseline 从 232/734 净降至 221/714；production 从
  99/287 降至 91/270，test 从 119/407 降至 116/404，compatibility 保持 14/40；Broker production 从
  17/53 降至 9/36，Store 保持 82/234。
- 执行清单 R04 完成；31 项最小审查清单已完成 7 项、剩余 24 项。正式进度仍为 75/82，PR-M11-12 未提前关闭。

## M11-12bc59 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | Broker default/all-target/all-feature check 通过；default POP message 26/26、buffer 10/10、revive 13/13 通过；all-feature POP message 26/26、buffer 12/12、revive 13/13 通过；Broker all-target/all-feature strict Clippy 通过 |
| BrokerRuntime composition suite | 62 passed、17 failed；经典 POP 构造、Store 写入、角色切换与 processor 接线通过；失败仍为已登记的 Lite 状态与 lifecycle probe 基线，未新增本切片目标失败 |
| reviewed baseline / fixtures | 正式 baseline 从 232/734 净降至 221/714；正常 guard、reviewed candidate compare 与 24/24 fixtures 通过，无 relocation、扩大的 governed debt 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、134/134 guard tests、dependency fixtures/baseline/target、release、8-profile/11-variant performance 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures。error architecture guard 仍仅发现未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖；`git diff --check` 在提交前复核 |

## M11-12bc60 实现

Broker Admin/control-plane root capability 随 Issue #8533 完成以下收口：

- 删除 `BrokerAdminRuntimeHandle` 及其完整 `ArcMut<BrokerRuntimeInner>` owner；Admin dispatcher、
  `Broker2Client` 与所有 leaf 改持显式 `BrokerAdminRuntime`，不再保活或解引用完整 runtime root。
- broker/store 配置归并到单一 `BrokerRuntimeConfigState`，通过 `ArcSwap` 原子发布同一代际；Admin 配置更新与
  controller role transition 使用 RCU 更新，避免并发更新用旧副本覆盖另一侧配置。
- controller、membership、minimum-broker、role transition 与 Store HA 操作改由
  `BrokerControllerRuntime`、`BrokerControllerState`、`BrokerMembershipState` 和窄 Store capability 组合；
  请求级 Tokio operation gate 保留原串行语义，普通状态快照只使用短 parking lock/原子发布。
- Admin 的 Topic/Subscription/Offset/HA/Consumer/Message 路径继续观察同一 live manager；增量 Topic 注册在发送前
  重新采样已提交 metadata generation。`ProducerManager` 的 broker config 改为共享 `ArcSwapOption`，clone 后仍能
  观察后续 Admin 配置代际。
- Store 兼容边界仍由既有 `LegacyEscapeStoreOwner` 承载；本切片没有用新 wrapper 隐藏 Store 债务，该 owner 仍计入
  R01/R09，必须由后续 Broker composition-root/Store facade 切片删除。
- reviewed baseline 从 221 identities / 714 occurrences 净降至 220 / 713；production 从 91/270 降至
  90/269，test 保持 116/404，compatibility 保持 14/40。Broker production 从 9/36 降至 8/35，Store 保持
  82/234；净删除 1 个 production identity/1 occurrence。16 个保留 occurrence 经临时 ADR-013 一对一指纹审核，
  无新增 identity 或提交态临时 approval。
- R01 仍未完成；31 项最小审查清单仍为已完成 7 项、剩余 24 项，正式进度仍为 75/82。下一切片
  M11-12bc61 继续 Broker 启动、注册、后台任务或 Local/Rocks 组合根 owner。

## M11-12bc60 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | default/all-target/all-feature check 通过；Admin 54/54、配置代际 3/3、control-plane 2/2、Producer live config 1/1、未绑定 Store role no-op 1/1、Store 角色切换 2/2 通过；Broker default 与 all-feature strict Clippy 通过 |
| Broker broad lib suite | 串行 default suite 未通过：645 passed、23 failed、1 ignored；失败全部属于已登记的 lifecycle/Lite 状态基线，少于此前登记集合；bc60 的 Admin/config/controller/Store 聚焦项全部通过，无新增目标路径失败 |
| reviewed baseline / fixtures | 正式 baseline 从 221/714 净降至 220/713；`python scripts/arc_mut_guard.py`、reviewed candidate compare、24/24 fixtures 与 67/67 guard tests 通过；16 个保留 occurrence 完成临时 ADR-013 一对一指纹审核，无新增 identity 或提交态临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| typed-error guard | 初次扫描发现本切片新增的 HA error stringification 后已改为 typed `HAResult` 传播；复扫仅剩未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条，新增 finding 为 0 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc61 实现

Broker controller bootstrap capability 随 Issue #8535 完成以下收窄：

- controller bootstrap、leader discovery/refresh、broker ID 申请与注册、heartbeat、replica metadata 和 membership
  synchronization 迁入显式 `BrokerControllerRuntime`；初始化和周期任务只捕获该 runtime，不再捕获或解引用完整
  `BrokerRuntimeInner`。
- controller runtime 仅组合 broker address、shutdown signal、controller client、controller/membership state、配置代际、
  registration capability 与窄 Store control-plane capability；leader 变化、角色切换与注册顺序保持原语义。
- `EscapeBridgeCapability` 新增 heartbeat offset snapshot 与 alive-replica 更新窄接口；未绑定 Store 时读取返回空值、
  写入返回 typed unavailable error，不暴露完整 Store owner。
- reviewed baseline 从 220 identities / 713 occurrences 降至 220 / 703；production 从 90/269 降至 90/259，
  test 保持 116/404，compatibility 保持 14/40。Broker production 从 8/35 降至 8/25，Store 保持 82/234；
  净删除 10 个 production occurrences，无 relocation、新增 identity 或临时 approval。
- R01 仍未完成；31 项最小审查清单仍为已完成 7 项、剩余 24 项，正式进度仍为 75/82。下一切片
  M11-12bc62 继续 Broker 注册、state getter 或 Local/Rocks 组合根 owner。

## M11-12bc61 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | default 聚焦集与 all-feature check 通过；all-feature controller 21/21、control-plane 3/3、Store fail-closed 1/1 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 串行 default suite 未通过：647 passed、23 failed、1 ignored；失败集合与 bc60 完全一致，仍为已登记的 lifecycle/Lite 状态基线；新增 controller 边界与动态多-controller/双-broker 回归全部通过，无新增目标路径失败 |
| reviewed baseline / fixtures | 正式 baseline 从 220/713 精确降至 220/703；`python scripts/arc_mut_guard.py`、reviewed candidate compare、24/24 fixtures 与 67/67 guard tests 通过；净删除 10 个 production occurrences，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | controller bootstrap 的一次性观察延迟由 `broker_runtime.rs` 随边界迁移到精确 controller bootstrap 文件，runtime guard 增加该单文件的 bounded-delay 分类；enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| typed-error guard | 复扫仅剩未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条；本切片新增 finding 为 0 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖；`git diff --check` 在提交前复核 |

## M11-12bc62 实现

Broker registration/state observation capability 随 Issue #8537 完成以下收窄：

- 新增显式 `BrokerRegistrationRuntime`，只组合 live config、Topic config/coordinator/mapping manager、
  `BrokerOuterAPI`、窄 Store capability、slave synchronization 与 shutdown signal；周期全量注册不再捕获完整
  `BrokerRuntimeInner`。
- Admin 增量和单 Topic 注册与周期全量注册复用同一窄 runtime；metadata snapshot、split registration、
  force/need-register 判断、oneway/timeout/compression/slave-acting-master 参数以及注册结果的 HA/master/order-topic
  更新语义保持不变。
- Producer/Consumer 统计 state observer 移入独立模块，只持共享 TopicConfigManager 与 Producer/Consumer manager；
  初始化顺序调整为 Topic manager 就绪后安装 observer，不再通过完整 runtime root 延迟访问。
- 删除 `broker_runtime.rs` 中重复的 registration helper、root type propagation 与旧 state getter；reviewed baseline
  从 220 identities / 703 occurrences 降至 220 / 697，production 从 90/259 降至 90/253，test 保持 116/404，
  compatibility 保持 14/40。Broker production 从 8/25 降至 8/19，Store 保持 82/234；净删除 6 个
  production occurrences，无 relocation、新增 identity 或临时 approval。
- R01 仍未完成；31 项最小审查清单仍为已完成 7 项、剩余 24 项，正式进度仍为 75/82。下一切片
  M11-12bc63 继续 BrokerRuntime 与 Local/Rocks/Store accessor 组合根 owner。

## M11-12bc62 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | default/all-target 与 all-feature check 通过；registration boundary 2/2、state observer 1/1、permission snapshot 1/1、controller failover/NameServer re-registration/Store HA 1/1 通过；Broker default 与 all-feature strict Clippy 通过 |
| Broker broad lib suite | 串行 default suite 未通过：648 passed、23 failed、1 ignored；失败集合与 bc61 完全一致，仍为已登记的 lifecycle/Lite 状态基线；新增 registration/state observer 回归全部通过，无新增目标路径失败 |
| reviewed baseline / fixtures | 正式 baseline 从 220/703 精确降至 220/697；`python scripts/arc_mut_guard.py`、reviewed candidate compare、24/24 fixtures 与 67/67 guard tests 通过；净删除 6 个 production occurrences，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| typed-error guard | 复扫仅剩未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条；本切片新增 finding 为 0 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖；`git diff --check` 在提交前复核 |

## M11-12bc63 实现

Broker background/observation boundary 随 Issue #8539 完成以下收窄：

- observability callbacks 不再克隆完整 `BrokerRuntimeInner`；Topic、Subscription、Producer/Consumer、POP/Ack、
  Timer 分别捕获自身共享 handle，Store/RocksDB/tiered-store 指标复用既有晚绑定 Store capability。
- daily stats、offset/filter/order persistence、Store lag、slave synchronization、NameServer address refresh 与 metadata
  refresh 等 scheduled work 只捕获 shutdown、config、manager、Store、slave 或 outer-API 窄能力。production
  `self.inner.clone()` 已清零；controller/registration/admin 边界保持此前显式 runtime/context 设计。
- `message_store`、`message_store_mut` 与 unchecked exclusive accessor 从暴露 `ArcMut<MS>` 改为普通 `&MS`/
  `&mut MS` 借用；删除无调用方的 unchecked shared accessor。底层单一兼容 Store owner 仍显式计入 R01/R09，
  本切片没有把构造或 owner 债务迁入 Store。
- reviewed baseline 从 220 identities / 697 occurrences 降至 220 / 693；production 从 90/253 降至 90/249，
  test 保持 116/404，compatibility 保持 14/40。Broker production 从 8/19 降至 8/15，Store 保持 82/234；
  净删除 4 个 production occurrences，无 relocation、新增 identity 或临时 approval。
- R01 仍未完成；31 项最小审查清单仍为已完成 7 项、剩余 24 项，正式进度仍为 75/82。下一切片
  M11-12bc64 迁移 test caller 并将 `BrokerRuntime.inner` 改为独占值。

## M11-12bc63 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | default/all-target 与 all-feature check 通过；background source boundary 1/1、all-feature observability config 2/2 通过；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 串行 default suite 未通过：649 passed、23 failed、1 ignored；失败集合与 bc62 完全一致，仍为已登记的 lifecycle/Lite 状态基线；新增 background boundary 测试通过，无新增目标路径失败 |
| observability feature matrix | CI 对齐的 default、`observability`、`otlp-metrics`、`otel-metrics,prometheus`、`otlp-traces`、`otlp-logs` 与 OTLP/Prometheus 全组合共 7 个 profile，其 workspace check、strict Clippy 和 `rocketmq-observability` 测试全部通过 |
| reviewed baseline / fixtures | 正式 baseline 从 220/697 精确降至 220/693；`python scripts/arc_mut_guard.py`、reviewed candidate compare、24/24 fixtures 与 67/67 guard tests 通过；净删除 4 个 production occurrences，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| typed-error guard | 复扫仅剩未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条；本切片新增 finding 为 0 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖；`git diff --check` 在提交前复核 |

## M11-12bc64 实现

Broker exclusive composition root 随 Issue #8541 完成以下收窄：

- `BrokerRuntime.inner` 从 `ArcMut<BrokerRuntimeInner<GenericMessageStore>>` 改为独占
  `Box<BrokerRuntimeInner<GenericMessageStore>>`；构造路径和 `inner_for_test` 同步改为独占值与普通可变借用，
  production 不再存在可克隆的完整 Broker root。
- 43 处 `inner_for_test().clone()` 全部删除。Admin handler 测试复用 owned `BrokerAdminRuntime`，ClientManage
  测试提取共享 Producer/Consumer manager 与 Topic manager，Lite subscription 测试提取 registry clone 与
  ConsumerOffset handle；测试不再通过长期 `&mut BrokerRuntimeInner` 跨处理器调用持有组合根。
- root 生命周期测试删除已失效的 `ArcMut::strong_count` 模拟，改由 production source contract 禁止
  `BrokerRuntimeInner`/`ArcMut` 捕获；MessageStore hook 仍保留 Store 自身的真实强引用前后不变合同。
- reviewed baseline 从 220 identities / 693 occurrences 降至 220 / 690；production 从 90/249 降至 90/246，
  test 保持 116/404，compatibility 保持 14/40。Broker production 从 8/15 降至 8/12，Store 保持 82/234；
  净删除 root field、root constructor 与 test accessor 返回类型中的 3 个 production occurrences，无 relocation、
  新增 identity 或临时 approval。
- R01 仍未完成；剩余 12 个 Broker production occurrence 全部属于 Local/Rocks 构造与 Store owner carrier，随
  R09～R16 删除。31 项最小审查清单仍为已完成 7 项、剩余 24 项，正式进度仍为 75/82。

## M11-12bc64 验证

| 命令 | 结果 |
|---|---|
| Broker check / focused / strict Clippy | default/all-target 与 all-feature check 通过；exclusive-root/source 生命周期合同 4/4、ClientManage 7/7、Admin handler 54/54 通过；Lite subscription 保持既有 3/9；Broker all-target/all-feature strict Clippy 通过 |
| Broker broad lib suite | 串行 default suite 保持 649 passed、23 failed、1 ignored；失败集合与 bc63 完全一致，仍为已登记的 lifecycle/Lite 状态基线，本切片新增失败为 0 |
| reviewed baseline / fixtures | 正式 baseline 从 220/693 精确降至 220/690；`python scripts/arc_mut_guard.py`、reviewed candidate compare、24/24 fixtures 与 67/67 guard tests 通过；净删除 3 个 production occurrences，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| typed-error guard | 复扫仅剩未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条；本切片新增 finding 为 0 |
| root workspace final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖；`git diff --check` 在提交前复核 |

## M11-12bc65 实现

Store mutable CommitLog facade 随 Issue #8543 完成以下收窄：

- 删除全仓零真实调用方的 `MessageStore::get_commit_log_mut_from_ref`，以及 `GenericMessageStore`、
  `LocalFileMessageStore`、`RocksDBMessageStore` 的 forwarding/实现；共享只读 `get_commit_log` 与独占
  `get_commit_log_mut(&mut self)` 行为保持不变。
- `base/message_store.rs` 增加 source contract，同时覆盖 trait、Generic、Local 与 Rocks 四个边界文件，禁止重新引入
  `&self -> &mut CommitLog` facade，并确认四层仍保留独占可变入口。
- 该接口是包含 `ArcMut` 字段的 LocalFile/RocksDB aggregate 被 guard 判定为 shared unsafe wrapper 的唯一共享引用
  可变逃逸。删除后 Local/Rocks concrete type、constructor、import、alias、DerefMut wrapper 与 downstream test 使用的
  传递债务真实退出；保留的显式 `ArcMut`、`ArcConsumeQueue` 与其他 `mut_from_ref` 仍继续计入 R01/R09～R16。
- reviewed baseline 从 220 identities / 690 occurrences 降至 138 / 506；production 从 90/246 降至
  62/168，test 从 116/404 降至 62/298，compatibility 保持 14/40。Broker production 从 8/12 降至
  4/8，Store 从 82/234 降至 58/160；净删除 82 identities/184 occurrences，无 relocation、新增 identity
  或临时 approval。
- 1.0.0 next-major 兼容台账记录零 consumer、迁移到 `get_commit_log_mut(&mut self)`/窄 capability 和禁止恢复
  unsafe facade 的决策。`main` 与候选 Store Rustdoc 公共 path 均为 384 且 fingerprint 相同；该方法的五个
  associated items 从 5 降为 0。冻结的全 workspace API snapshot 相对当前 `main` 已有 23 个待审差异，本切片
  没有重置 snapshot baseline。
- R01、R09～R16 均仍未完成；31 项最小审查清单仍为已完成 7 项、剩余 24 项，正式进度仍为 75/82。

## M11-12bc65 验证

| 命令 | 结果 |
|---|---|
| Store check / focused / lib / strict Clippy | default/all-target 与 all-feature/all-target check 通过；新增 source contract 1/1、Store default lib 504/504、all-feature legacy adapter 9/9 通过；Store all-target/all-feature strict Clippy 通过 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | `--apply-reviewed-reductions` 候选从 220/690 精确降至 138/506；正式 baseline 后 `python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过；82 个 removed identity 全部属于 Local/Rocks shared-wrapper 传递闭包，保留 identity 仅有 Local `mut_from_ref` 从 4 降至 3，无新增 identity、relocation、指纹替换或临时 approval |
| public API / Rustdoc | `main` 与候选 `rocketmq-store` 公共 path 均为 384，fingerprint 均为 `eead0cefaf841d19e5635b32a502e9517870f88ec222fb396a0cba8707caaa25`；目标 associated items 5→0；Store Rustdoc 通过。全 workspace frozen snapshot 如实为 review-required：相对旧 baseline 共 23 个既有包级差异、8 个 public-path 差异，本切片不重置 baseline |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| typed-error guard | 仅剩未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条；本切片新增 finding 为 0 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy、Store `cargo doc --no-deps` 与 `git diff --check` 通过；Windows linker stdout、既有 future-incompatibility note 和 4 条既有 Store Rustdoc HTML warning 不受 `-D warnings` 管辖 |

## M11-12bc66 实现

LocalFile 后台任务边界随 Issue #8545 完成以下收窄：

- Reput reader、dispatcher 与 one-shot 不再接收或保存完整 `ArcMut<LocalFileMessageStore>`；改为显式
  `ReputRuntimeContext`，仅持配置、最大延迟级别、不可变 delay table、Store stats 与消息到达通知能力。
- 消息到达 listener 由完整 Store 查询改为 live capability。替换或清除 listener 会被既有 Reput context 观察到；
  callback 在标准 `RwLock` guard 释放后执行，不把锁带入外部回调。
- scheduled self-check 不再捕获完整 LocalFile root，只捕获既有 CommitLog 与 ConsumeQueueStore child handle；
  wiring readiness 仍在注册与启动入口检查，检查顺序和故障语义不变。
- Reput shutdown 在受管 `TaskGroup` 完成后释放 `inner` runtime context，同时保留 `reput_from_offset` 以支持原有
  重启语义；reader 创建失败路径也立即释放 context。
- reviewed baseline 从 138 identities / 506 occurrences 降至 138 / 501；production 从 62/168 降至
  62/164，test 从 62/298 降至 62/297，compatibility 保持 14/40。Store production 从 58/160 降至
  58/156；R10 从 6/38 降至 6/34。净删除 4 个 production occurrence 与 1 个 test occurrence，未新增 identity。
- 7 个保留 occurrence 因函数签名变化产生指纹替换，均经 ADR-013 一对一临时审核；approval 仅位于忽略的
  `target/architecture-refactor/M11-12bc66/`，不提交正式 baseline，也没有 relocation 或扩大 debt。
- R10 仍未完成：Local root 与 CommitLog、ConsumeQueue、Timer、HA/recovery 的其余组合环仍在后续 R09～R16
  收口；31 项执行清单仍为完成 7 项、剩余 24 项，正式进度仍为 75/82。
- scanner 已登记一个分类限制：`CleanCommitLogService::run` 的 2 个 production `mut_from_ref` 调用因同一 struct
  含 `#[cfg(test)]` 字段而被归入 test。该债务仍须在 R10/R13 清理，不能据当前 production 分类提前宣称清零。

## M11-12bc66 验证

| 命令 | 结果 |
|---|---|
| Store check / focused / lib / strict Clippy | all-target/all-feature check 通过；Reput 11/11、source contract 1/1、multi-dispatch 2/2、default lib 508/508、all-feature lib 514/514 通过；Store all-target/all-feature strict Clippy 通过 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 138/501；`python scripts/arc_mut_guard.py`、candidate compare、24/24 fixtures 与 67/67 guard tests 通过；净删除 4 个 production 与 1 个 test occurrence，7 个保留指纹替换完成临时一对一审核，无新 identity、relocation 或已提交 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| typed-error guard | 仅复现未改动文件中的既有 4 条 finding：Client retry token 2 条、Schedule source stringification 2 条；本切片新增 finding 为 0 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy、Store all-feature `cargo doc --no-deps` 与 `git diff --check` 通过；Windows linker stdout、既有 future-incompatibility note 和 4 条既有 Store Rustdoc HTML warning 不受 `-D warnings` 管辖 |

## M11-12bc67 实现

CommitLog cleanup 边界随 Issue #8547 完成以下收窄：

- `CleanCommitLogService`、`CleanConsumeQueueService` 与 `CorrectLogicOffsetService` 不再持有
  `ArcMut<CommitLog>`，改持 `CommitLogCleanupHandle`；该 capability 只克隆标准 Arc mapped-file generation 和文件尺寸，
  scheduled cleanup/min-offset 路径不再解引用 legacy CommitLog root。固定延迟 scheduler、manual retry budget、WAL pin、
  delete interval、batch limit 与日志语义不变。
- MappedFileQueue 的增量 create/delete 发布集中到 ArcSwap RCU/CAS helper；cleanup capability 与 writer 共享同一
  generation。optimized load、destroy 等 authoritative replacement 保持 `&mut` 独占 lifecycle，并通过
  `replace_mapped_files_exclusive` 明确 writer-quiesced 前提，不能把覆盖式 replacement 宣称为并发 merge-safe。
- cleanup 在旧 snapshot 完成文件 destroy 后，以候选文件 identity 在最新代际过滤；若 segment publication 抢先提交，
  RCU closure 自动重试，因此不会丢失新 tail 或把已删除文件重新加入集合。确定性 barrier 回归强制覆盖 CAS retry。
- source contract 固化三个 service 只持窄 cleanup capability，CommitLog legacy cleanup facade 恢复 `&mut self`，
  MappedFileQueue 只允许 RCU 增量发布和一处命名清楚的 exclusive store；M06 exact contract 同步固定新的 storage/
  lifecycle adapter，并保留 mutation tests。
- scanner 的 `#[cfg(test)]` 范围解析按 item/statement/field 结构结束；field type 仍识别泛型角括号，但默认值和 const
  表达式中的 `<`/`>` 不再被误当作无限 generic depth。跨 identity relocation 除 path/symbol/kind/owner/reason/ADR/item
  外同时比较 `remove_by`，防止把 M03 债务迁入 M11 identity 延期；loader 对 `from_identity` 有直接回归。
- reviewed baseline 从 138/501 降至 137/493：production 62/159、test 61/294、compatibility 14/40；Broker production
  4/8、Store production 58/151。相对 bc66 净删除 1 个 identity/8 occurrences，其中三个 service 的完整 CommitLog
  owner type/constructor 共删除 6 个 production occurrences。
- 相邻 cleanup-handle 初始化改变了同一 `try_new` 内 CommitLog constructor fingerprint，经 ADR-013 临时一对一审核；
  approval 仅在忽略的 `target/` 中用于 candidate review，未提交到仓库。R10、R13 仍未完成，31 项清单仍为完成 7 项、剩余 24 项。

## M11-12bc67 验证

| 命令 | 结果 |
|---|---|
| Store check / focused / lib / strict Clippy | all-target/all-feature check 与 strict Clippy 通过；MappedFileQueue 13/13、CleanCommitLogService 7/7、narrow-capability source contract 1/1、default lib 510/510、all-feature lib 516/516、CommitLog load 7/7（1 ignored）、store-local mapped-file lifecycle 8/8 通过 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 137/493；`python scripts/arc_mut_guard.py`、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；同 identity fingerprint relocation 仅使用忽略的临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance policy、60/60 architecture tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 Store all-feature `cargo doc --no-deps` 通过；Windows linker stdout、既有 future-incompatibility note 和 4 条既有 Store Rustdoc HTML warning 不受 `-D warnings` 管辖 |

## M11-12bc68 实现

Mapped-file flush 边界随 Issue #8549 完成以下收窄：

- CommitLog 直接拥有 `MappedFileQueue`；`DefaultFlushManager`、GroupCommitService、FlushRealTimeService、
  CommitRealTimeService 与 FlushConsumeQueueService 改持 `MappedFileQueueFlushHandle`，不再保存或克隆完整 queue owner。
- flush handle 只暴露 mapped-file lookup、max offset、commit、flush 和进度/时间戳读取。它克隆标准 Arc generation 与
  `MappedFileQueueRuntimeState`；runtime-state clone 共享同一组原子水位和串行锁，不复制 commit/flush 进度。
- `MappedFileQueue::commit` 与 `try_flush` 委托到同一窄 handle 实现，避免 owner 路径与 worker 路径出现两套状态机；
  segment publication、store timestamp、least-pages 和 thorough-interval 语义保持不变。
- `DefaultFlushManager::new` 收窄为 crate 内构造入口。CommitLog 是唯一 composition root，外部不能把完整 queue owner
  重新注入 worker；source contract 固化 production/test 均不允许恢复 `ArcMut<MappedFileQueue>`。
- M06 exact contract 增加 flush capability、runtime-state clone sharing 及 mutation 回归，并同步校正已漂移的
  TopicConfig 标准 Arc 精确合同。
- reviewed baseline 从 137/493 降至 133/476：production 从 62/159 降至 60/149，test 从 61/294
  降至 59/287，compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 58/151
  降至 56/141。相对 bc67 净删除 4 identities/17 occurrences（production 2/10、test 2/7），无 relocation、
  新增 identity 或临时 approval。
- R13 从 5/21 降至 3/11；`default_flush_manager.rs` production ArcMut 已清零，剩余债务属于
  CommitLog/dispatcher/flush-manager owner，不能提前标记 R13 完成。31 项清单仍为完成 7 项、剩余 24 项，正式进度
  仍为 75/82。

## M11-12bc68 验证

| 命令 | 结果 |
|---|---|
| Store check / focused / lib / strict Clippy | all-target/all-feature check 与 strict Clippy 通过；flush-manager source contract、MappedFileQueue flush-handle、MappedFileQueue 13/13、DefaultFlushManager 13/13、default lib 512/512、all-feature lib 518/518 通过；bc67 cleanup source contract 同步改为 CRLF/LF 归一化并按最后一个 test module 截断 |
| Store-local / CommitLog integration | mapped-file lifecycle 8/8、runtime-state 3/3、CommitLog load 7/7（1 ignored）通过；M06 lifecycle/storage/runtime-state/CommitLog root 精确合同与 mutation 共 8/8 通过 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 133/476；`python scripts/arc_mut_guard.py`、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 4 identities/17 occurrences，无 relocation、新 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance policy、60/60 architecture tests、7/7 telemetry tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy、Store all-feature `cargo doc --no-deps` 与 `git diff --check` 通过；Windows linker stdout、既有 future-incompatibility note 和 4 条既有 Store Rustdoc HTML warning 不受 `-D warnings` 管辖 |

## M11-12bc69 实现

CommitLog 子服务所有权随 Issue #8551 完成以下收窄：

- LocalFileMessageStore 直接拥有 `CommitLogDispatcherDefault` registry；初始 ConsumeQueue/Index、可选 Compaction 与
  Tiered dispatcher 继续在 composition root 内按原顺序注册，`MessageStore::get_dispatcher_list` 仍返回 owner slice。
- registry 每次独占注册后通过 ArcSwap 发布不可变 dispatcher generation。`CommitLogDispatchHandle` 只克隆发布器，
  CommitLog 与 Reput reader/dispatcher 不再保存 `ArcMut<CommitLogDispatcherDefault>`。
- 同步 dispatch 对当前 snapshot 迭代；异步单条/批量 dispatch 在 await 前用 `load_full` 固定 generation，因此不持
  同步锁跨 await，并保持同一 generation 内的 dispatcher 顺序和 backpressure 语义。
- CommitLog 直接拥有 `DefaultFlushManager`；start、shutdown 和 graceful shutdown 继续由 `&mut CommitLog` 独占驱动，
  共享 disk-flush 与 runtime-info 仍使用 `&self`。零外部调用的 `CommitLog::new` 收窄为 crate 内 composition 入口。
- source contract 与 M06 exact contract 禁止恢复完整 dispatcher/flush-manager ArcMut owner；行为回归证明在 handle
  创建后追加和前插 dispatcher 都会按 `[first, appended]` 顺序被观察。
- reviewed baseline 从 133/476 降至 132/466：production 从 60/149 降至 59/139，test 保持 59/287，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 56/141 降至 55/131。相对 bc68
  净删除 1 identity/10 occurrences。
- 3 个保留 Reput `ArcMut<CommitLog>` occurrence 因相邻 dispatcher 参数/字段改变指纹，经 ADR-013 临时一对一审核；
  approval 仅位于忽略的 `target/`，不提交正式仓库。R10 从 6/29 降至 6/23，R13 从 3/11 降至 2/7；两项仍未完成。
  31 项清单仍为完成 7 项、剩余 24 项，正式进度仍为 75/82。

## M11-12bc69 验证

| 命令 | 结果 |
|---|---|
| Store check / focused / lib / strict Clippy | all-target/all-feature check 与 strict Clippy 通过；dispatcher publication、CommitLog child source contract、cleanup source contract、M06 CommitLog root 正负合同通过；default lib 514/514、all-feature lib 520/520 通过 |
| CommitLog integration | CommitLog load 7/7（1 ignored）通过；dispatcher 顺序回归证明 handle 创建后的 append/prepend publication 被观察为 `[first, appended]` |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 132/466；`python scripts/arc_mut_guard.py`、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 1 identity/10 occurrences，3 个同 identity Reput 指纹 relocation 仅使用忽略的临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance policy、60/60 architecture tests、7/7 telemetry tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy、Store all-feature `cargo doc --no-deps` 与 `git diff --check` 通过；Windows linker stdout、既有 future-incompatibility note 和 4 条既有 Store Rustdoc HTML warning 不受 `-D warnings` 管辖 |

## M11-12bc70 实现

CommitLog LocalStore back-reference 随 Issue #8553 完成以下收口：

- `CommitLogStoreContext` 只持标准 Arc running flags、replica atomic、StoreStats、不可变 delay metadata 与
  `ArcSwapOption<GeneralHAService>`；LocalFileMessageStore 完成 HA init 后原子发布 snapshot，CommitLog 读路径不再需要
  `ArcMut<LocalFileMessageStore>`。
- controller confirm、HA ack、put stats 与 async group transfer 只读取窄 context；async HA path 在 await 前通过
  `load_full` 取得稳定 service generation，不持同步锁跨 await。warm mapped-file unlock 直接使用 MappedFile 能力。
- 四条 normal/abnormal、standard/optimized recovery 入口删除完整 Store root 参数；empty/truncate/controller-clamp
  completion 通过 CommitLog 已拥有的 ConsumeQueueStore 与 WAL offsets 完成，保持原 recovery 顺序和水位语义。
- Local delay table 从 `ArcMut<BTreeMap<_, _>>` 改为不可变 Arc；CommitLog tests 删除 production glob 传播的 ArcMut
  import 与两个 helper 返回类型 occurrence，保留 constructor 通过全限定路径构造并由 trait-object test harness 隐藏。
- source contract 与 M06 exact composition contract 禁止恢复 LocalStore back-reference、ArcMut delay table 或旧 adapter 字段；
  18 条适用 recovery integration、controller/HA 与全部 Store lib 回归覆盖窄 context 行为。
- reviewed baseline 从 132/466 降至 128/453：production 从 59/139 降至 57/129，test 从 59/287 降至
  57/284，compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 55/131 降至 53/121，
  Store test 从 47/200 降至 45/197。相对 bc69 净删除 4 identities/13 occurrences。
- 唯一保留 test constructor 因改用全限定路径而改变 occurrence id，经 ADR-013 临时一对一 relocation 审核；approval
  仅位于忽略的 `target/`，不提交正式仓库。R10 从 6/23 降至 6/20；R13 从 2/7 降至 0/0 并完成。
  31 项清单现为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc70 验证

| 命令 | 结果 |
|---|---|
| Store check / source / M06 / lib | all-target/all-feature check 通过；CommitLog store-context source contract 与 M06 CommitLog root 正负合同通过；default lib 515/515、all-feature lib 521/521 通过 |
| CommitLog recovery | 适用 recovery integration 18/18 通过；`file_store_vs_rocksdb_behavior_parity_after_restart` 在本分支与未含 bc70 的基线 `c2c707808` 均以相同 RocksDB compatibility CQ 空快照失败，明确记录为既有非零项，不计作通过 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 128/453；`python scripts/arc_mut_guard.py`、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 4 identities/13 occurrences，1 个同 identity test constructor occurrence relocation 仅使用忽略的临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance policy、43/43 dependency tests、6/6 release tests、11/11 performance tests、7/7 telemetry tests与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy、Store all-target/all-feature strict Clippy 与 all-feature `cargo doc --no-deps` 通过；Windows linker stdout、既有 future-incompatibility note 和 4 条既有 Store Rustdoc HTML warning 不受 `-D warnings` 管辖 |

## M11-12bc71 实现

CommitLog long-lived read capability 随 Issue #8555 完成以下收窄：

- `MappedFileQueueReadHandle` 只持 ArcSwap mapped-file generation、mapped-file size 与共享 runtime state，提供读取、
  offset、flush watermark、roll 与 self-check 能力，不暴露 allocation、recovery、truncate 或 destruction。
- `CommitLogReadHandle` 组合该只读句柄、不可变 Store/Broker 配置、`CommitLogStoreContext` 与共享原子 runtime state；
  facade 与 handle 共用同一 confirm resolver，保持 controller、duplication、SyncFlush 与 AsyncFlush 语义一致。
- Reput、后台索引重建、scheduled self-check、compaction payload 与 tiered dispatch resolver 改持只读 capability，
  不再捕获完整 `ArcMut<CommitLog>`；公开 tiered compatibility resolver 保持原签名。
- `CommitLogRuntimeState` 通过 `Arc` 共享 confirm publication；MappedFileQueue handle 创建后仍能观察 generation replacement
  与 progress 更新。source contract 禁止长期读者恢复完整 CommitLog owner。
- M06 exact contract 同步登记 bc67 的 generation RCU、bc70 的自有 ConsumeQueue recovery completion 与本切片 read handle，
  删除已经失真的完整 LocalStore recovery 签名/side-effect 预期；160 项正负 mutation contract 全量通过。
- reviewed baseline 从 128/453 降至 128/447：production 从 57/129 降至 57/123，test 保持 57/284，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 53/121 降至 53/115。
  相对 bc70 净删除 6 个 production occurrences，无新增 identity。
- 唯一保留的 Local composition-root constructor occurrence 因相邻 handle 构造改动而改变 fingerprint，经 ADR-013 临时
  一对一 relocation 审核；approval 仅位于忽略的 `target/`，不提交正式仓库。R10 从 6/20 降至 6/14，仍未完成；
  31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc71 验证

| 命令 | 结果 |
|---|---|
| Store check / lib / source / M06 | all-target/all-feature check、Store-local runtime-state 5/5、default lib 517/517、all-feature lib 523/523 通过；CommitLog/read-handle source contract 与 M06 160/160 通过 |
| Store strict Clippy / Rustdoc | Store-local 与 Store all-target/all-feature strict Clippy、Store all-feature `cargo doc --no-deps` 通过；4 条既有 Rustdoc HTML warning 未扩大 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 128/447；`python scripts/arc_mut_guard.py`、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 6 个 production occurrences，1 个同 identity composition-root occurrence relocation 仅使用忽略的临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance policy、43/43 dependency tests、6/6 release tests、11/11 performance tests、7/7 telemetry tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc72 实现

Local commit-log maintenance 随 Issue #8557 完成以下收窄：

- `MappedFileQueue::get_last_mapped_file_mut_start_offset`、`try_create_mapped_file`、`truncate_dirty_files` 与
  `reset_offset` 改为共享引用 API；create/truncate/reset 通过共享 `MappedFileQueueRuntimeState` maintenance lock
  串行化，并继续通过 ArcSwap RCU 发布 mapped-file generation。
- 同一 offset 的并发 create 在锁内检查最新 generation 并复用已发布的 `DefaultMappedFile`，避免两个调用者顺序取得锁后
  重复创建、重复发布；并发回归验证两个线程得到相同 Arc identity 且 generation 仅含一个文件。
- `CommitLog` 的 last-file create、reset 与 truncate facade 同步改为 `&self`；LocalFileMessageStore 的
  `reset_write_offset`、`truncate_files` 与 `get_last_mapped_file` 删除 3 处 `mut_from_ref`。
- source contract 与 M06 exact mutation contract 固定共享引用签名、maintenance lock、同 offset publication reuse 与
  CommitLog facade，负向 mutation 会拒绝恢复 `&mut self`、删除锁或移除重复发布保护。
- reviewed baseline 从 128/447 降至 127/444：production 从 57/123 降至 56/120，test 保持 57/284，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 53/115 降至 52/112。
  相对 bc71 净删除 1 个 production identity/3 occurrences，无 relocation、新增 identity 或临时 approval。
- R10 从 6/14 降至 5/11，仍未完成；31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc72 验证

| 命令 | 结果 |
|---|---|
| Store check / lib / source / M06 | all-target/all-feature check、create/reset/truncate/source 定向 4/4、default lib 519/519、all-feature lib 525/525 通过；M06 exact positive/negative contract 160/160 通过 |
| Store strict Clippy / Rustdoc | Store-local 与 Store all-target/all-feature strict Clippy、Store all-feature `cargo doc --no-deps` 通过；4 条既有 Rustdoc HTML warning 未扩大 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 127/444；`python scripts/arc_mut_guard.py`、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 1 个 production identity/3 occurrences，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance policy、43/43 dependency tests、6/6 release tests、11/11 performance tests、7/7 telemetry tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12bc73 实现

Local CommitLog owner 随 Issue #8561 完成以下收窄：

- LocalFileMessageStore 将完整 `CommitLog` 从 `ArcMut<CommitLog>` 改为直接独占字段，构造完成后把 facade 移入 Local
  composition root，不再克隆或共享完整 WAL owner。
- Reput、后台索引、自检、compaction、tiered resolver、cleanup、flush 与 dispatcher 继续只持 bc67～bc72 已提取的
  read/cleanup/flush/dispatch/store-context capability，没有子服务重新获得完整 CommitLog。
- recovery、load、start、shutdown、destroy 与 `MessageStore::get_commit_log_mut` 保持 Local 根独占可变借用；只读 facade
  直接返回 `&CommitLog`，不通过 wrapper 间接解引用。
- source contract 与 M06 exact mutation contract 固定直接字段、direct move、窄 handle 提取和独占可变 facade；负向 mutation
  会拒绝恢复 `ArcMut<CommitLog>`、wrapper construction、完整 facade clone 或间接 mutable access。
- reviewed baseline 从 127/444 降至 127/442：production 从 56/120 降至 56/118，test 保持 57/284，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 52/112 降至 52/110。
  相对 bc72 净删除 2 个 production occurrences，无 relocation、新增 identity 或临时 approval。
- R10 从 5/11 降至 5/9，仍未完成；31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc73 验证

| 命令 | 结果 |
|---|---|
| Store check / lib / source / M06 | all-target/all-feature check、CommitLog owner/read-handle source 定向测试、default lib 520/520、all-feature lib 526/526 通过；M06 exact positive/negative contract 最终 160/160、Local composition 5/5 通过。一次 15 分钟外层超时与一次错误放置 mutation 导致的 Python `NameError` 均未计为通过，修正后全量重跑退出码为 0 |
| Store strict Clippy / Rustdoc | Store-local 与 Store all-target/all-feature strict Clippy、Store all-feature `cargo doc --no-deps` 通过；4 条既有 Rustdoc HTML warning 未扩大 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 均为 127/442；`python scripts/arc_mut_guard.py`、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 2 个 production occurrences，无 relocation、新增 identity 或临时 approval |
| runtime / architecture guards | enforcing runtime audit、dependency fixtures/baseline/target、release、8-profile/11-variant performance policy、43/43 dependency tests、6/6 release tests、11/11 performance tests 与 AGENTS routing 通过；目标 compatibility 35/35、test edge 3/3、release topology 32/32、66 comparisons/0 failures。telemetry semantic guard 未通过且其测试在 setup 阶段 0 项运行：PR #8559 新增的 `LOG_FILTER_ACTIVE`、`LOG_FILTER_AUDIT_FAILURE_TOTAL`、`LOG_FILTER_AUTO_RESTORE_FAILURE_TOTAL`、`LOG_FILTER_EXPIRY_TIMESTAMP_SECONDS`、`LOG_FILTER_RELOAD_TOTAL`、`LOG_FILTER_ROLLBACK_FAILURE_TOTAL` 未登记；在干净 main `147047780` 与本分支复现相同结果，bc73 未改 observability/registry，该非零项未计为通过 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |

## M11-12g1 telemetry registry gate repair 实现

Issue #8563 将 bc73 验证暴露的当前 main telemetry 基线回归独立收敛：

- `LOG_FILTER_RELOAD_TOTAL`、`LOG_FILTER_ACTIVE`、`LOG_FILTER_EXPIRY_TIMESTAMP_SECONDS`、
  `LOG_FILTER_AUDIT_FAILURE_TOTAL`、`LOG_FILTER_AUTO_RESTORE_FAILURE_TOTAL` 与
  `LOG_FILTER_ROLLBACK_FAILURE_TOTAL` 补入 Rust metric catalog，使用精确 counter/gauge kind、unit、
  `MetricSource::Observability` 与空 label set。
- generator 从源码 inventory 重建 canonical registry 与 violation fixture；signal 总量从 130 更新为 136，metric
  总量从 119 更新为 125，并纳入 PR #8559 同时新增的 `service`、`source` 两项 attribute，总量从 66 更新为 68。
- Python source contract 固定 125/4/7 inventory 与 136/68 registry，并逐项验证 6 个 log-filter signal 的 stable、
  operational privacy、零 attribute、cardinality budget 1 与 aggregate sampling 元数据。
- 该切片只同步 catalog/registry 治理元数据，不修改指标名称、label、exporter、log-filter runtime 或 ArcMut owner；
  reviewed baseline 保持 127/442，R10 保持 5/9，31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12g1 telemetry registry gate repair 验证

| 命令 | 结果 |
|---|---|
| generator / semantic guard | generator 生成 125 metrics/136 signals/68 attributes；`python scripts/telemetry_semantic_guard.py` 通过 |
| telemetry contracts | Python 8/8 通过；Rust catalog 5/5 通过，含 6 个 log-filter metric exact contract |
| observability all-feature tests | lib 131/131、architecture guard 11/11、error compatibility 2/2、log-filter resolution 4/4、logging bootstrap 10/10 通过；2 项既有显式 ignored 保持 |
| validation scope | 变更仅涉及 always-on catalog/registry 元数据，使用 all-features superset 覆盖；已启动的 21 命令逐 feature workspace 矩阵按用户要求停止且未计为通过，未重复运行 6 个中间 feature 组合 |
| root workspace final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过 |

## M11-12bc74 实现

Local store self-retention 随 Issue #8565 完成以下收窄：

- `LocalFileMessageStore` 删除 `Option<ArcMut<LocalFileMessageStore>>` 字段及恢复完整 root 的 helper，以
  `root_dependencies_wired` 只记录一次性组合完成状态，不再从 Local 根重新取得自身共享句柄。
- `set_message_store_arc` 只把根句柄下发到既有 ConsumeQueue/Timer 兼容边界，并构造 Default/AutoSwitch HA
  pending child；Default 大变体以独占 `Box` 暂存，避免 enum 尺寸放大且不引入新的共享 owner。
- `init` 从 pending slot 一次性取出 HA child，在原生命周期边界包装为既有 `GeneralHAService`、初始化并发布；
  scheduled task、Reput 与 start 只检查 wiring 状态，未 wiring 时继续 fail closed。
- source contract 与 M06 exact mutation contract 禁止恢复完整 Local self field/helper、跳过 pending handoff 或移除
  wiring 完成标记，并固定 Default/AutoSwitch 两条初始化路径。
- reviewed baseline 从 127/442 降至 127/440：production 从 56/118 降至 56/116，test 保持 57/284，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 52/110 降至 52/108。
  相对 bc73 净删除 2 个 production occurrences，无新增 identity。
- 两种 HA wrapper constructor 与 setter 参数共 3 个保留 occurrence 经 ADR-013 临时一对一 relocation 审核；
  approval 仅位于忽略的 `target/architecture-refactor/M11-12bc74/`，不提交正式仓库或扩大 debt。
- R10 从 5/9 降至 5/7，仍未完成；31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc74 验证

| 命令 | 结果 |
|---|---|
| focused Store / M06 | Local self-retention、unwired init、Timer wiring 定向 Rust 回归 3/3 通过；M06 exact positive/negative mutation contract 2/2 通过。三条首次以不完整名称配合 `--exact` 的命令运行 0 项，未计为通过，修正后均实际运行 1 项 |
| RocksDB specialized gates | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过。首次 Store Clippy 发现 `large_enum_variant`，改为独占 Box 后完整六项重跑通过，首次非零结果未计为通过 |
| reviewed baseline / fixtures | 正式 baseline 与 bc73→bc74 reviewed candidate 均为 127/440；直接 guard、精确 compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 2 个 production occurrences，3 个 retained occurrence 经临时 ADR-013 一对一审核，无新增 identity 或提交态 approval |
| runtime / root final gates | enforcing runtime audit、`cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| reduced validation scope | 未重复运行 default/all-feature Store lib 全套、dependency/release/performance/telemetry 矩阵、Rustdoc 或 AGENTS routing；这些边界未被本切片修改，由定向行为/架构合同、RocksDB 专项和最终 workspace strict Clippy 覆盖 |

## M11-12bc75 实现

Default HA client runtime ownership 随 Issue #8567 完成以下收窄：

- `DefaultHAClient.inner` 从 `ArcMut<Inner>` 改为标准 `Arc<Inner>`；连接循环只共享原子状态、异步锁、Notify、
  flow monitor 与既有 LocalStore 兼容句柄，不再通过共享可变 root 驱动单 owner service task。
- `Inner` 删除从未安装过连接的 read/write stream，以及与 ReaderTask/WriterTask 重复的 dispatch/read/backup/report
  buffer；真实连接 half 与解析/上报 buffer 仍由每个 connection generation 的 reader/writer task 独占。
- `connect_master`、`close_master`、`close_master_and_wait` 与 `notify_shutdown` 收窄为共享 receiver；TaskGroup 的
  connection child、错误监督、重连、shutdown cancellation/await 顺序保持不变。
- Rust source contract 与 M06 exact mutation contract 固定标准 Arc root、task-local buffer 和当前
  `ReplicationStateRoot`/`HAAckedReplicaSnapshot` 边界，禁止恢复 ArcMut runtime 或重复未使用状态。
- reviewed baseline 从 127/440 降至 126/437：production 从 56/116 降至 55/113，test 保持 57/284，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 52/108 降至 51/105。
  相对 bc74 净删除 1 个 production identity/3 occurrences，无新增 identity。
- 唯一保留的 `Inner.default_message_store` occurrence 因同一结构体内删除相邻字段而更新指纹，经 ADR-013 临时
  一对一 relocation 审核；approval 仅位于忽略的 `target/architecture-refactor/M11-12bc75/`，不提交正式仓库。
- R15 从 9/29 降至 8/26，仍未完成；31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc75 验证

| 命令 | 结果 |
|---|---|
| focused Store / M06 | Default HA client 定向 Rust 回归 4/4 通过；M06 exact positive/negative mutation contract 2/2 通过。M06 首轮暴露 production/test 切片混用以及 AutoSwitch standard Arc、HA snapshot re-export 两项过期断言，按当前 main 真实边界修复后重跑通过，先前非零结果未计为通过 |
| reviewed baseline / fixtures | 正式 baseline 与 bc74→bc75 reviewed candidate 均为 126/437；直接 guard、精确 compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 1 个 production identity/3 occurrences，1 个 retained occurrence 经临时 ADR-013 一对一审核，无新增 identity 或提交态 approval |
| runtime / root final gates | enforcing runtime audit、`cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| reduced validation scope | 未单独重复 Store strict Clippy（最终 workspace Clippy 已覆盖）；未运行 RocksDB 六项专项、Store 全量 lib、dependency/release/performance/telemetry 矩阵、Rustdoc 或 AGENTS routing，因为本切片未修改对应行为、feature、公共 API、文档命令或项目路由 |

## M11-12bc76 实现

ConsumeQueueStore root ownership 随 Issue #8569 完成以下收窄：

- cloneable `ConsumeQueueStore` 从 `ConsumeQueueStoreRoot<ArcMut<Inner>>` 改为标准 `Arc<Inner>`；queue table 与
  offset operator 继续使用既有同步机制，不再因组合根可克隆而共享引用可变访问完整内部状态。
- 唯一晚绑定的 `ArcMut<LocalFileMessageStore>` compatibility handle 放入短 `parking_lot::RwLock<Option<_>>`；
  setter 只发布句柄，lookup、type check 与 queue construction 读取后立即 clone，guard 不跨文件访问或异步边界。
- 现有未 wiring 时的 fail-closed error/expect 语义、Simple/Batch/RocksDB CQ 映射、recovery 与 timestamp 查询保持不变；
  Rust source contract 与 M06 exact mutation contract 禁止恢复共享可变 queue root 或无锁晚绑定。
- reviewed baseline 从 126/437 降至 126/435：production 从 55/113 降至 55/111，test 保持 57/284，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 51/105 降至 51/103。
  相对 bc75 净删除 2 个 production occurrences，无新增 identity。
- 5 个保留的 LocalStore/queue trait `ArcMut` occurrence 因同 item 内的 locked snapshot 调整而更新指纹，经
  ADR-013 临时一对一 relocation 审核；approval 仅位于忽略的 `target/architecture-refactor/M11-12bc76/`。
- R12 从 17/36 降至 17/34，仍未完成；31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc76 验证

| 命令 | 结果 |
|---|---|
| focused Store / M06 | ConsumeQueueStore source、recovery、Simple/Batch/RocksDB CQ mapping 定向 Rust 回归 10/10 通过；M06 ConsumeQueue exact contract 1/1 通过。M06 首轮负向字符串命中新增 Rust source test，限定到 production 切片后重跑通过，先前非零结果未计为通过 |
| reviewed baseline / fixtures | 正式 baseline 与 bc75→bc76 reviewed candidate 均为 126/435；直接 guard、精确 compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 2 个 production occurrences，5 个 retained occurrence 经临时 ADR-013 一对一审核，无新增 identity 或提交态 approval |
| root final gates | `cargo fmt --all -- --check` 与 workspace all-target/all-feature strict Clippy 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| reduced validation scope | 未单独重复 Store strict Clippy（最终 workspace Clippy 已覆盖）；未运行 runtime audit（未修改 TaskGroup/runtime/lifecycle 边界）、RocksDB 六项专项（未修改 backend/feature 行为）、Store 全量 lib、dependency/release/performance/telemetry 矩阵、Rustdoc 或 AGENTS routing |

## M11-12bc77 实现

ConsumeQueue trait-object ownership 随 Issue #8571 完成以下收窄：

- `ArcConsumeQueue` 从 `ArcMut<Box<dyn ConsumeQueueTrait>>` alias 改为标准 `Arc` 包装的每队列
  `parking_lot::RwLock<Box<dyn ConsumeQueueTrait>>` handle；clone 只共享同步句柄，不再产生 unchecked mutable
  trait-object escape，也不引入 Store 全局锁。
- Store queue root、LocalFileMessageStore、Timer 与 Broker admin/schedule 调用方显式选择 read/write guard；mutable
  dispatch/recovery/load/truncate/destroy 使用 write，查询/迭代/flush 使用 read。传入现有 queue 借用的只读 lifecycle
  facade 直接调用对象，避免持 read guard 后再次查表取得同队列锁。
- 借用型 CQ iterator 始终受 read guard 生命周期保护；Broker delayed-message schedule 改为逐条获取下一条 unit，
  在 delivery await 前释放 guard，保持 offset/min/max 修正与逐条投递语义。
- M06 source contract 固定每队列 RwLock newtype、显式读写入口、无旧 alias，以及 schedule guard 不跨 await；
  queue recovery、Timer、LMQ multi-dispatch 与 Broker admin query 定向行为回归覆盖读写路径。
- reviewed baseline 从 126/435 降至 103/391：production 从 55/111 降至 34/71，test 从 57/284
  降至 55/280，compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 51/103
  降至 30/63。相对 bc76 净删除 23 identities/44 occurrences（production 21/40、test 2/4），无
  relocation、新增 identity 或临时 approval。
- R09 从 8/13 降至 4/7、R10 从 5/7 降至 3/4、R11 从 5/7 降至 3/4、R12 从 17/34
  降至 4/6，均仍未完成；31 项执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

## M11-12bc77 验证

| 命令 | 结果 |
|---|---|
| affected compile / behavior / source | Store 与 Broker all-target/all-feature check 通过；ConsumeQueueStore 10/10、Timer `process_once` 15/15、LMQ multi-dispatch 3/3、Broker consume-queue query 1/1、M06 contract 2/2 通过。M06 首轮因格式化后的换行导致精确字符串断言失败，改为受限正则后重跑通过；先前非零结果未计作通过 |
| RocksDB specialized gate | Store/Broker `rocksdb_store` all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过。Broker Clippy 首轮发现 schedule match condition block，提取首条 unit 绑定后重跑通过；先前非零结果未计作通过 |
| reviewed baseline / fixtures | 正式 baseline 与 reviewed candidate 的 semantic set 均为 103/391；直接 guard、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 23 identities/44 occurrences，无 relocation、新增 identity 或临时 approval |
| runtime / root final gates | enforcing runtime audit、`cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| reduced validation scope | 未重复 Store/Broker 全量 lib、dependency/release/performance/telemetry 矩阵、Rustdoc 或 AGENTS routing；这些边界未修改，受影响行为由定向回归、RocksDB 专项、runtime audit 和最终 workspace strict Clippy 覆盖 |

## M11-12bc78 实现

ConsumeQueue 完整 LocalStore compatibility carrier 随 Issue #8573 完成以下收窄：

- `ConsumeQueueStore` 的晚绑定字段从 `ArcMut<LocalFileMessageStore>` 改为由标准 `Arc` 组成的窄 context；
  context 仅包含 MessageStoreConfig、topic table、RunningFlags、StoreCheckpoint 与既有 `CommitLogReadHandle`。
- simple `ConsumeQueue` 删除泛型 `ArcMut<MS>` owner，只持窄 context 和标准 `Weak<Inner>` queue lookup；lookup
  提供 find-or-create 语义以保持 LMQ 首次建队与已有 offset dispatch 行为，同时不形成 Store/queue 强引用环。
- `CommitLogReadHandle` 增加只读 timestamp lookup，MappedFileQueue 的时间边界选择和 CQ unit store-time 查询
  不再借用完整 CommitLog 或通过 MessageStore downcast 获取 queue store。
- M06 source contract 固定无 production `ArcMut<LocalFileMessageStore>`/`ArcMut<MS>` carrier、标准 Weak lookup、
  read handle timestamp；7 个 single CQ、10 个 CQ store、3 个 LMQ multi-dispatch 与 2 个时间查询定向测试覆盖行为。
- reviewed baseline 从 103/391 降至 99/385：production 从 34/71 降至 30/65，test 保持 55/280，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 30/63 降至 26/57。
  相对 bc77 净删除 4 个 production identities/6 occurrences，无 relocation、新增 identity 或临时 approval。
- R12 从 4/6 降至 0/0 并完成；31 项执行清单现为完成 9 项、剩余 22 项，正式进度仍为 75/82。

## M11-12bc78 验证

| 命令 | 结果 |
|---|---|
| affected compile / behavior / source | Store all-target/all-feature check 与 strict Clippy 通过；single CQ 7/7、CQ store 10/10、LMQ multi-dispatch 3/3、时间查询 2/2、M06 contract 3/3 通过。LMQ 首轮发现 Weak lookup 缺少首次建队语义，改为窄 find-or-create handle 后重跑通过；先前非零结果未计作通过 |
| reviewed baseline / fixtures | 正式 baseline 与 promoted candidate 的 semantic set 均为 99/385；直接 guard、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 4 identities/6 occurrences，无 relocation、新增 identity 或临时 approval |
| RocksDB specialized gate | Store/Broker `rocksdb_store` exact-feature all-target strict Clippy 通过；foundation 82/82、semantics 9/9、Broker RocksDB 21/21、POP consumer 4/4 通过 |
| root final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| reduced validation scope | 未重复 Store/Broker 全量 lib、独立 Broker/Store all-feature check、runtime audit、dependency/release/performance/telemetry 矩阵、Rustdoc 或 AGENTS routing；未修改 runtime/lifecycle、公开 API、构建或路由边界，行为由 22 项定向回归、RocksDB 专项、所有权 guard 与最终 workspace strict Clippy 覆盖 |

## M11-12bc79 实现

Timer delivery 的共享可变逃逸随 Issue #8575 完成以下收窄：

- CommitLog 单条与批量追加入口从 `&mut self` 改为 `&self`；mapped-file generation 创建/发布继续受既有
  maintenance lock 保护，topic queue offset 分配与 append 继续分别受 topic-queue lock 和 put-message lock 串行。
- LocalFileMessageStore 提取单条/批量共享追加实现，保留 `MessageStore` 的 `&mut self` 兼容签名作为窄委托；
  hook、LMQ offset、StoreStats 与 Reput notification 行为保持原路径。
- Timer rolled/due delivery 直接调用共享追加实现，删除两处完整 LocalStore `mut_from_ref`；新增并发共享追加
  回归证明同一 Store 上两个 future 获得不同物理 offset，Timer `process_once_*` 覆盖正常、滚动、删除与限流路径。
- M06 source contract 固定 Timer production 无 `mut_from_ref`、LocalStore 兼容委托与 CommitLog 共享 receiver。
- reviewed baseline 从 99/385 降至 98/383：production 从 30/65 降至 29/63，test 保持 55/280，
  compatibility 保持 14/40；Broker production 保持 4/8，Store production 从 26/57 降至 25/55。
  相对 bc78 净删除 1 个 production identity/2 occurrences，无 relocation、新增 identity 或临时 approval。
- R14 从 3/7 降至 2/5，仍未完成；31 项执行清单保持完成 9 项、剩余 22 项，正式进度仍为 75/82。

## M11-12bc79 验证

| 命令 | 结果 |
|---|---|
| affected compile / behavior / source | Store all-target/all-feature `cargo check` 通过；并发共享追加 1/1、Timer `process_once_*` 15/15、单条/批量 EOF roll 重试 2/2、M06 contract 5/5 通过。M06 首轮因 production 切片误在较早的 test-only helper 处截断而失败，改为按 `mod tests` 边界切分后重跑通过；先前非零结果未计作通过 |
| reviewed baseline / fixtures | 正式 baseline 与 `--prune-resolved` candidate 的 semantic set 均为 98/383；直接 guard、candidate compare、27/27 fixtures 与 78/78 guard tests 通过；净删除 1 个 production identity/2 occurrences，无 relocation、新增 identity 或临时 approval |
| root final gates | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过；Windows linker stdout 与既有 future-incompatibility note 不受 `-D warnings` 管辖 |
| reduced validation scope | 未重复 package strict Clippy（最终 workspace Clippy 已覆盖）、Store/Broker 全量 lib、RocksDB 六项专项、runtime audit、dependency/release/performance/telemetry 矩阵、Rustdoc 或 AGENTS routing；未修改 RocksDB feature/backend、TaskGroup/runtime/lifecycle、构建或路由边界，追加与 Timer 行为由 18 项定向 Rust 回归（含并发）、5 项 source contract、所有权 guard 与最终 workspace strict Clippy 覆盖 |

## 剩余切片与 Gate

2026-07-23 盘点将执行工作固化为 31 个最小可审查单元：16 个 production owner、2 个
test/compatibility、7 个 M10/Phase 3 动态验收与签署、6 个 M12；R02、R03、R04、R05、R06、R07、R08、R12、R13 已完成，当前剩余 22 个，正式进度仍为 75/82。
完整逐项 checklist 见 `docs/plans/architecture-refactor-migration/REMAINING-TASKS.md`。

1. Broker runtime root 已改为独占 `Box<BrokerRuntimeInner>`，production/test 完整 root clone 均已清零；bc65 删除 Local/Rocks concrete unsafe-wrapper 传播后，仅剩显式 `ArcMut` Store 组合根 capability carrier（4/8），随 R09～R16 Store owner 安全化删除。
2. Store 其余 StoreHandle/Timer 与 HA service/actor（25/55）；BrokerStats observer、ConsumeQueueExt 显式锁 owner、ConsumeQueueStore 标准 Arc root/窄 context、ConsumeQueue trait-object 每队列 RwLock handle、simple queue Weak find-or-create lookup、Timer shared append、HA replication-state callback、未共享 HA child direct ownership、Default HA client 标准 Arc runtime/task-local buffer、commit-to-flush 窄唤醒能力、HA confirm/epoch 原子发布、HA connection runtime handle、CommitLog shared disk-flush/recovery、标准 Arc cleanup/mapped-file flush/dispatcher/store-context/read capability、auto-switch client construction、single delegate Store owner、LocalStore back-reference、Local 完整 self retention、共享引用可变 CommitLog maintenance、完整 CommitLog owner 与 Local Reput/index/self-check/compaction/tiered 完整 CommitLog owner 已退出。
3. Production `WeakArcMut` 已清零；继续迁移 test/compatibility 中受控使用并移除其余 nightly feature。公开 `arc_mut.rs`/re-export 的 destructive 删除受 next-major 两轮弃用与 Release Manager/HUMAN Gate 约束，不能静默重置 public API baseline。
4. 对同一候选快照执行 stable feature matrix、Miri/Loom 可用切片、soak/SLO fault、dashboard/runbook、动态
   Kind/K3d/container、M10 固定硬件和 Human Gate。

任何切片失败都只回滚对应独立 PR，不扩大 baseline，不删除 durability/fault 证据，也不把 fixture 当作动态 PASS。
