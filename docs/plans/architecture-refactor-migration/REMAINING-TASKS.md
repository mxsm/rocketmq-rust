# 架构重构剩余任务盘点

> 盘点日期：2026-07-21
> 代码基线：Issue #8454 / M11-12bc24 完成后
> 统计规则：82 个顶层 `PR-Mxx-yy` 工作包与 M11-12 内部实施切片分开统计，禁止重复计数。

## 结论

正式工作包已完成 75/82，尚余 7 个：正在实施的 PR-M11-12，以及尚未开始的 PR-M12-01～PR-M12-06。
Phase 1、Phase 2 和 10 个目标边界 crate 已完成；当前主要工程量已不在 crate 拆分，而在 Broker/Store 共享可变
owner 清零、compatibility 删除和同一候选快照验收。

| 口径 | 已完成 | 剩余 | 说明 |
|---|---:|---:|---|
| 顶层 PR 工作包 | 75 | 7 | PR-M11-12 + PR-M12-01～06 |
| 里程碑 | 9 | 3 个未关闭 | M10 待验收、M11 实施中、M12 未开始 |
| Phase Gate | 2 | 2 | Phase 3、Phase 4 |
| 目标边界 crate | 10 | 0 | 根 workspace 已为目标 32 package |

## PR-M11-12 剩余实现

Issue #8454 后 reviewed ArcMut baseline 为 346 identities / 936 occurrences：production 188/458、test
144/438、compatibility 14/40。production 全部分布在 Broker 与 Store。

| owner | 剩余 identity / occurrence | 完成条件 |
|---|---:|---|
| Broker | 85 / 184 | transaction bridge、Producer/ColdData admin leaf、Schedule hook、put-message preflight、ConsumerOrderInfoManager、TopicRouteInfoManager、MessageArrivingListener、ClientHousekeepingService、HA diagnostics/control/min-broker transition、BatchMq lock/unlock、SubscriptionGroup/MessageRelated/Offset/Consumer Admin 与未编译 V2 示例残留已退出 leaf-level 完整 runtime/store owner；LiteLifecycle 只读 API 已改为普通借用；继续让显式 Store 兼容边界、BrokerRuntime aggregate carrier、其余 admin/processor/service 不再传播不安全共享可变 owner |
| Store | 103 / 274 | BrokerStats observer、ConsumeQueueExt owner、HA notification/connection registry capability 与未共享 HA child 已收窄；其余 MessageStore、CommitLog/Flush、queue、Rocks/Timer 与 HA service/actor 改为独占 owner、标准 Arc/Weak 或显式 actor/锁边界 |
| compatibility | 14 / 40 | 先迁移 Store 对 `WeakArcMut` 的剩余使用；公开 `ArcMut`/`WeakArcMut`/`SyncUnsafeCellWrapper` 删除必须满足 next-major 两轮弃用与独立 HUMAN/Release Manager 批准，不能通过重置 API baseline 提前关闭 |

建议按以下最小可审查批次继续推进；它们是 PR-M11-12 的内部切片，不增加 82 个顶层工作包总数：

1. Broker aggregate：收窄 `BrokerRuntimeInner`、processor variant 和启动 carrier（Broker 当前为 85/184）；Schedule hook、put-message preflight、ConsumerOrderInfoManager、TopicRouteInfoManager、MessageArrivingListener 与 ClientHousekeepingService 强保活边已拆除，HA diagnostics/control/min-broker transition、BatchMq、SubscriptionGroup、MessageRelated、Offset 与 Consumer handler 已改为父层请求期借用，未编译 V2 示例残留已清理；继续清理其他 admin/processor leaf。
2. Broker leaf：完成其余 admin/processor/revive/slave/offset leaf owner；transaction bridge 已由 M11-12bc4 收窄，Producer/ColdData admin handler 已由 M11-12bc5 改持 live registry/standard Arc capability，Schedule hook 已由 M11-12bc6 改持三项显式能力。
3. Store WAL：收口 Local/Rocks MessageStore、CommitLog 与 Flush manager，并替换 transaction 的直接 Store 兼容 owner。
4. Store queue：ConsumeQueueExt 已改用显式锁 owner；继续收口其余 ConsumeQueue、queue store、index/mapped-file carrier。
5. Store timer/HA：BrokerStats observer、HA notification service、connection registry 查询与未共享 client/connection child 已退出多余 owner；继续收口 Timer、Default/General/AutoSwitch HA service 与 actor 回指。
6. compatibility/stable：迁移剩余测试/兼容调用方和 Store `WeakArcMut`；按 next-major/HUMAN 窗口处理公开 facade，并替换 `sync_unsafe_cell`、`async_fn_traits`、`unboxed_closures` 等 nightly surface。
7. 候选快照 Gate：冻结同一 commit，执行 stable feature matrix、Miri/Loom 可用切片、soak/SLO fault、动态
   Kind/K3d/container、dashboard/runbook/rollback，并完成 `[ARCH]`、`[REV]`、`[TEST]`、`[HUMAN]` 签署。

上述 7 个批次是依据当前代码热点形成的执行计划，不是“还剩 7 个正式工作包”。实际 PR 数可因每个切片的风险与审查
大小拆分，但完成目标不能通过合并批次而减少。

M11-12bc4 没有虚报数量下降：transaction bridge 删除了完整 `BrokerRuntimeInner` 访问，offset、Topic registration、
EscapeBridge 使用窄标准 `Arc` capability；原有 2 个 ArcMut identity / 3 个 occurrence 被搬到显式
`TransactionMessageStore` 兼容边界，因此总量保持 418/1,051。该边界仍计入未完成债务，必须由 Store 批次删除。

M11-12bc5 将 Producer 查询与 ColdData 管理 handler 从完整 runtime owner 收窄为 live producer registry 和标准
`Arc<ColdDataCgCtrService>`。production 净删除 4 identities / 6 occurrences；ColdData 测试 glob 不再传递导入
ArcMut，额外删除 test 1/1，因此 reviewed 总量降至 413/1,044，无 relocation，compatibility 不增。

M11-12bc6 将 MessageStore 的 Schedule hook 从完整 runtime owner 收窄为 `MessageStoreConfig`、可选
`TimerMessageStore` 与标准 `Arc<ScheduleMessageService>`；helper 只接收配置、timer 借用和实时最大延迟级别，
注册不再复制 runtime owner。production 净删除 4 identities / 7 occurrences，测试 glob 额外删除 test 1/1，
因此 reviewed 总量降至 408/1,036，无 relocation，compatibility 不增。

M11-12bc7 将 `BrokerStats` 从完整 MessageStore owner 收窄为标准 `Arc<BrokerStatsManager>`，兼容构造只在边界
提取 manager 后立即释放 Store handle；Broker 的 Local/Rocks 组合根直接注入 manager。HA connection-state notification
service 从具体 `LocalFileMessageStore` owner 收窄为标准 `Arc<MessageStoreConfig>`，DefaultHAService 直接注入配置代际。
production 净删除 6 identities / 9 occurrences，因此 reviewed 总量降至 402/1,027，Store 降至 116/299；
test 与 compatibility 不增，无 relocation。

M11-12bc8 将 `ConsumeQueueExt` 的 mapped-file queue owner 从 `ArcMut<MappedFileQueue>` 改为标准
`Arc<Mutex<MappedFileQueue>>`；load/recover/truncate/put/flush/destroy 和查询均通过显式锁串行，共享实例回归证明
两个 ext handle 观察同一 queue 代际。production 净删除 4 identities / 6 occurrences，test 净删除 2/2，
因此 reviewed 总量降至 396/1,019，Store 降至 112/293；compatibility 不增，无 relocation。

M11-12bc9 删除 `HAService::get_connection_list`，group transfer 只取得 owned ack snapshot，notification 只按 remote
address 查询 scalar state，连接 owner 留在 DefaultHAService registry 内。notification 请求不再在首次非终态轮询时丢失，
master 路径的双重 `take` 失效同步修复；目标状态、允许的 shutdown、替换或超时才消费请求。production 净删除
2 identities / 5 occurrences，因此 reviewed 总量降至 394/1,014，Store 降至 110/288；test 与 compatibility 不增，
无 relocation。

M11-12bc10 将 Store-owned put-message preflight hook 从完整 `ArcMut<MessageStore>` owner 收窄为仅含 shutdown、
running flags 与 commit-log lock timestamp 的原子只读能力，注册后 Store 强引用计数保持不变；自定义 Store 的默认
能力 fail-closed。LiteLifecycle 的 max-offset/existence 查询同时从 `Option<&ArcMut<MS>>` 收窄为 `Option<&MS>`，
调用方只传播普通借用。production 净删除 4 identities / 6 occurrences，test 净删除 1/1，因此 reviewed 总量
降至 389/1,007，Broker 降至 116/235；Store 110/288 与 compatibility 14/40 不增，无 relocation。

M11-12bc11 将 `GeneralHAClient` 的 Default/AutoSwitch payload、`AutoSwitchHAClient` 的 Default delegate 与
`GeneralHAConnection` 的 Default/AutoSwitch optional child 改为直接 owned value。这些 child 从未独立 clone 或逃逸，
生命周期可变操作已有外层 `&mut self` 独占访问；外层 HA service、connection registry 与 task 的
`WeakArcMut<GeneralHAConnection>` 回指保持不变。production 净删除 7 identities / 14 occurrences，因此 reviewed
总量降至 382/993、production 降至 219/509、Store 降至 103/274；test 149/444 与 compatibility 14/40 不增，
无 relocation。

M11-12bc12 将 `ConsumerOrderInfoManager` 从完整 `ArcMut<BrokerRuntimeInner>` back-reference 收窄为存储根目录、
标准 `Arc<TopicConfigManager>` 与共享 subscription-group live table，删除 `MessageStore` 泛型和 4 个无调用方
mutable/unchecked/setter accessor。配置路径仍由初始化时的 Broker 配置决定，topic/group 自动清理继续观察共享
live table。production 净删除 2 identities / 3 occurrences，test glob 净删除 1/1，因此 reviewed 总量降至
379/989、production 降至 217/506、Broker 降至 114/232、test 降至 148/443；Store 103/274 与 compatibility
14/40 不增，无 relocation。

M11-12bc13 删除 `rocketmq-broker/src/processor_v2_migration_example.rs`：该 tracked standalone source 从未进入
Broker module tree，也从未由 Cargo/测试编译；Remoting 已有 canonical V2 implementation、complete example 与
integration tests，因此删除不会改变 Broker runtime wiring。production 净删除 2 identities / 7 occurrences，test
净删除 1/2，因此 reviewed 总量降至 376/980、production 降至 215/499、Broker 降至 112/225、test 降至
147/441；Store 103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc14 将 `TopicRouteInfoManager` 从完整 `ArcMut<BrokerRuntimeInner>` back-reference 收窄为共享
`BrokerOuterAPI`、轮询间隔与可选父 `TaskGroup`，删除 `MessageStore` 泛型和无调用方 unchecked/setter 入口；无
`ServiceContext` 时保留 ambient Tokio runtime fallback。production 净删除 2 identities / 3 occurrences，因此
reviewed 总量降至 374/977、production 降至 213/496、Broker 降至 110/222；test 147/441、Store 103/274 与
compatibility 14/40 不增，无 relocation。

M11-12bc15 将 Store-owned `NotifyMessageArrivingListener` 从完整 `ArcMut<BrokerRuntimeInner>` back-reference 收窄为
Pull hold、POP 与 Notification processor 的三项标准 `Weak` handle，注册移至三项 owner 初始化后；late notification
在 teardown owner 已释放时安全跳过。production 净删除 2 identities / 3 occurrences，因此 reviewed 总量降至
372/974、production 降至 211/493、Broker 降至 108/219；test 147/441、Store 103/274 与 compatibility 14/40
不增，无 relocation。

M11-12bc16 将 `ClientHousekeepingService` 从完整 `ArcMut<BrokerRuntimeInner>` back-reference 收窄为仅暴露 scan/close
的 Producer/Consumer narrow handle、标准 `Arc<BrokerStatsManager>` 与可选父 `TaskGroup`，删除 `MessageStore` 泛型；周期扫描和
channel connect/close/exception/idle 统计继续作用于同一 live state，幂等启动、有界关闭与 parent lifecycle 语义不变。
production 净删除 2 identities / 3 occurrences，test glob 净删除 1/1，因此 reviewed 总量降至 369/970、production
降至 209/490、Broker 降至 106/216、test 降至 146/440；Store 103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc17 让 Admin dispatch 复用 broker-config handler 已登记的 runtime owner，并将 `GetBrokerHaStatusHandler` 与
`BrokerEpochCacheHandler` 改为无状态 leaf；两者删除 runtime field、Clone 与 struct-level `MessageStore` 泛型，请求期间
只接受普通共享 runtime 借用且不新增父层 owner。响应、缺失 Store/HA 与 controller-mode 错误语义保持不变。production 净删除 4 identities /
6 occurrences，test glob 净删除 1/1，因此 reviewed 总量降至 364/963、production 降至 205/484、Broker 降至
102/210、test 降至 145/439；Store 103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc18 将 `ResetMasterFlushOffsetHandler` 与 `UpdateBrokerHaHandler` 改为无状态 leaf；两者删除 runtime field、Clone
与 struct-level `MessageStore` 泛型，Admin dispatch 从 broker-config handler 现有 owner 取得普通 runtime 借用。
master/slave、offset update、HA address exchange 语义保持不变且不新增父层 owner。production 净删除 4 identities /
6 occurrences，因此 reviewed 总量降至 360/957、production 降至 201/478、Broker 降至 98/204；test 145/439、
Store 103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc19 将 `BatchMqHandler` 改为无状态 leaf，删除 runtime field、Clone 与 struct-level `MessageStore` 泛型；
Admin dispatch 在 lock/unlock 请求期间传入普通 runtime 借用。严格锁 fan-out 只 clone `BrokerOuterAPI` 进入各副本
future，不再传播完整 runtime，quorum/timeout/local lock/unlock 语义保持不变。production 净删除 2 identities /
3 occurrences，因此 reviewed 总量降至 358/954、production 降至 199/475、Broker 降至 96/201；test 145/439、
Store 103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc20 将 `SubscriptionGroupHandler` 改为无状态 leaf，删除 runtime field、Clone 与 struct-level `MessageStore`
泛型；写请求通过 broker-config handler 的现有 owner 取得请求期独占借用，读请求取得共享借用。未被 dispatch 调用的
重复 unlock 方法和 imports 同步删除。production 净删除 2 identities / 3 occurrences，test glob 净删除 1/1，
因此 reviewed 总量降至 355/950、production 降至 197/472、Broker 降至 94/198、test 降至 144/438；Store
103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc21 将 `MessageRelatedHandler` 改为无状态 leaf，删除 runtime field 与 struct-level `MessageStore` 泛型；
search-offset、query-consume-queue 和 POP rollback 从 broker-config handler 的现有 owner 取得请求期共享借用，只有
resume-check-half-message 重入写 Store 使用独占借用，静态主题重写沿用同一共享借用。production 净删除 2 identities /
3 occurrences，因此 reviewed 总量降至 353/947、production 降至 195/469、Broker 降至 92/195；test 144/438、
Store 103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc22 将 `OffsetRequestHandler` 改为无状态 leaf，删除 runtime field、Clone 与 struct-level `MessageStore`
泛型；offset/delay/subscription/cleanup 请求从 broker-config handler 的现有 owner 取得请求期共享借用，static-topic
max/min/earliest 重写沿用同一借用，unsupported RocksDB 路径不取得 runtime。production 净删除 2 identities /
3 occurrences，因此 reviewed 总量降至 351/944、production 降至 193/466、Broker 降至 90/192；test 144/438、
Store 103/274 与 compatibility 14/40 不增，无 relocation。

M11-12bc23 将 `NotifyMinBrokerChangeIdHandler` 从完整 runtime owner 收窄为仅保留 broker-id/address 状态锁；Admin
dispatch 为 minimum-broker 角色切换请求传入父层现有 owner 的独占借用，special-service 与 master offline/online
路径直接传播该借用并删除 `mut_from_ref`。production 净删除 3 identities / 5 occurrences，因此 reviewed 总量
降至 348/939、production 降至 190/461、Broker 降至 87/187；test 144/438、Store 103/274 与 compatibility
14/40 不增，无 relocation。

M11-12bc24 将 `ConsumerRequestHandler` 从完整 runtime owner 收窄为无状态 leaf；Admin dispatch 为 consumer
connection/stats/status/subscription/time-span、request-mode、running-info 与 offset clone 请求传入父层共享借用，
仅 reset-offset 路径传入独占借用。production 净删除 2 identities / 3 occurrences，因此 reviewed 总量降至
346/936、production 降至 188/458、Broker 降至 85/184；test 144/438、Store 103/274 与 compatibility 14/40
不增，无 relocation。

## PR-M12 剩余工作包

| 工作包 | 目标 |
|---|---|
| PR-M12-01 | Evidence normalization 与 Knowledge Graph |
| PR-M12-02 | 受控 RAG |
| PR-M12-03 | 多领域确定性诊断 |
| PR-M12-04 | 冻结 Plan contract 并证明无副作用 |
| PR-M12-05 | 独立 Apply 边界，仅在 Human 批准后实施 |
| PR-M12-06 | Eval、red-team 与离线 fallback |

## 不计入 7 个工作包、但必须关闭的验收项

- M10 固定硬件 baseline/candidate 与 HUMAN 性能验收。
- Phase 3 动态 fault/cloud 证据和四方签署。
- Phase 4 Plan/Apply 安全边界、AI 离线可用性与四方签署。
- 35 条目标 DAG compatibility/composition ledger 按 R1、next-major、long-term 窗口只降不增；其中长期批准边
  不等同于当前 ArcMut compatibility 债务。

只有 PR-M11-12 的 production/public compatibility ArcMut 清零且候选快照 Gate 完整通过后，顶层进度才能从
75/82 更新为 76/82；完成六个 M12 工作包并通过 Phase 4 Gate 后才是 82/82。
