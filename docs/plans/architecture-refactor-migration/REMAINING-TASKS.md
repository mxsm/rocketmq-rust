# 架构重构剩余任务盘点

> 盘点日期：2026-07-23
> 代码基线：Issue #8649 / M11-12bc114 候选完成后
> 统计规则：82 个顶层 `PR-Mxx-yy` 工作包与 M11-12 内部实施切片分开统计，禁止重复计数。

## 结论

正式工作包已完成 75/82，尚余 7 个：正在实施的 PR-M11-12，以及尚未开始的 PR-M12-01～PR-M12-06。
Phase 1、Phase 2 和 10 个目标边界 crate 已完成；Broker production ArcMut 已清零，当前主要工程量已不在 crate
拆分，而在 Store 剩余 facade/root carrier、compatibility 删除和同一候选快照验收。

| 口径 | 已完成 | 剩余 | 说明 |
|---|---:|---:|---|
| 顶层 PR 工作包 | 75 | 7 | PR-M11-12 + PR-M12-01～06 |
| 里程碑 | 9 | 3 个未关闭 | M10 待验收、M11 实施中、M12 未开始 |
| Phase Gate | 2 | 2 | Phase 3、Phase 4 |
| 目标边界 crate | 10 | 0 | 根 workspace 已为目标 32 package |
| 最小可审查执行单元 | 19 | 12 | 31 项清单中的 R01～R08、R10～R17、R22～R24 已完成 |

按当前代码热点、兼容窗口和候选快照 Gate 进一步拆分的清单共 **31 个最小可审查单元**：
16 个 production owner 收口、2 个 test/compatibility 收口、7 个 M10/Phase 3 动态验收与签署、6 个 M12
工作包；R01、R02、R03、R04、R05、R06、R07、R08、R10、R11、R12、R13、R14、R15、R16、R17、R22、R23、R24 已完成，当前剩余 12 个。31 是执行估算，不是新的正式工作包总数；相邻单元可以合并到同一 PR，遇到高风险
owner 也可以继续拆分，但 7 个正式工作包的统计口径保持不变。

## PR-M11-12 剩余实现

Issue #8649 / M11-12bc114 候选后 reviewed ArcMut baseline 保持 20 identities / 58 occurrences：production
6/12、test 1/7、compatibility 13/39。bc100 让 fast-failure 改持 `PutMessagePreflight`，bc101 让
`BrokerAdminRuntime` 只持 `Weak<EscapeBridge>`，bc102 进一步让 `BrokerRuntimeInner` 成为唯一 Store 生命周期强
owner，EscapeBridge 只保存标准 `Weak` provider；bc103 删除 Admin/processor 完整 Store 可变租约，仅保留三个具名
Admin Store 操作；bc104 又让普通单条、批量和 Admin append 通过私有强类型共享端口执行，不再克隆完整可变
Store carrier；bc105 让 controller role-change 不再跨 await 持完整 Store clone，并把 read-mode/topic-delete
收窄为 owner 具名同步操作；bc106 删除 composition-root lifecycle lease，init/hook/start/shutdown 统一先解绑弱
provider，再通过 `Arc::get_mut` 取得独占 owner，shutdown 还会在绝对 deadline 内等待已接纳读租约退出；
bc107 最终删除私有 `LegacyEscapeStoreOwner<ArcMut<_>>`，Broker 改为直接持有标准 Arc Store，动态 role/read-ahead
通过 Store 原子运行态发布；bc108 又让 RocksDB composition root 以 Box 独占 LocalFile backend，并删除零调用的
完整 root clone accessor；bc109 将 RocksDB semantics helper 与 16 个 Broker Store-capability 测试模块迁到
`OwnedMessageStore`。production 现全部位于 Generic/Local/Timer 的公开兼容面，test 余量仅为 ArcMut facade
自身的兼容性测试。bc110 保留 Remoting GAT API 并以 concrete/boxed named future 删除
`impl_trait_in_assoc_type`，同时删除 Controller 已无必要的 `arbitrary_self_types`；仓库 nightly feature
属性从 8 个降至 4 个。bc111 保留 Runtime 四个公开 `_async` 调度入口，以 stable owned-future 转发层替换
重复的 lending `AsyncFnMut` scheduler，并批量迁移 Runtime、Client、Broker 调用方；Runtime
`async_fn_traits`/`unboxed_closures` 删除后，精确 nightly 余量降至 R18 的 2 个 `sync_unsafe_cell`。bc112
以 stable `parking_lot::RwLock` backing 保留 ArcMut 兼容名称，删除最后两处 gate 和过时 benchmark；nightly
feature 清零，stable default 与完整 all-target/all-feature workspace 矩阵通过，R22 完成。

| owner | 剩余 identity / occurrence | 完成条件 |
|---|---:|---|
| Broker | 0 / 0 | bc107 删除私有 `LegacyEscapeStoreOwner<ArcMut<_>>`，Broker 直接持有标准 `Arc<OwnedMessageStore>`；EscapeBridge/Admin 只保留 Weak provider，请求期读 lease 和 append port 只升级标准 Arc。read-mode/topic-delete/role-sync 使用线程安全窄运行态，生命周期仍先解绑 provider 再由 `Arc::get_mut` 证明独占。Broker production ArcMut 已清零，R01 完成 |
| Store | 6 / 12 | RocksDB composition root 已以 Box 独占 LocalFile backend，R11 清零；其余 production 项仅为 `GenericMessageStore` 2/5、LocalFile 旧 wiring 入口 2/2、Timer 旧完整 Store carrier 2/5，均属于公开兼容窗口。其他 BrokerStats/ConsumeQueue/HA/WAL/flush/queue/Local/Rocks/Timer 活跃 owner 已改为独占 owner、标准 Arc/Weak 或显式 capability/锁边界 |
| compatibility | 13 / 39 | production Store `WeakArcMut` 与活跃 test/bench caller 已清零；1/7 test 余量是 ArcMut facade 自身的兼容性测试。bc112 仅稳定 backing，不删除公开 facade；`ArcMut`/`WeakArcMut`/`SyncUnsafeCellWrapper` 删除必须满足 next-major 两轮弃用与独立 HUMAN/Release Manager 批准，不能通过重置 API baseline 提前关闭 |

建议按以下最小可审查批次继续推进；它们是 PR-M11-12 的内部切片，不增加 82 个顶层工作包总数：

1. Broker aggregate 已完成（0/0）：`BrokerRuntime.inner` 独占 `Box<BrokerRuntimeInner>`，production/test 完整 runtime root clone 均已清零；Broker 直接持有标准 Arc Store，Admin/EscapeBridge 只持 Weak provider，请求期读/append capability 只升级标准 Arc；生命周期由解绑 provider + `Arc::get_mut` 保持独占，read-mode/topic-delete/role-sync 由线程安全窄运行态承担。
2. Broker leaf：完成其余 admin/processor/revive/offset leaf owner；transaction bridge 已由 M11-12bc4 收窄，Producer/ColdData admin handler 已由 M11-12bc5 改持 live registry/standard Arc capability，Schedule hook 已由 M11-12bc6 改持三项显式能力，Topic Admin 已由 M11-12bc31 改为无状态 leaf，slave metadata synchronization 已由 M11-12bc50 改持显式 policy/弱 provider/晚绑定 capability。
3. Store WAL：commit worker 已只持 `Notify` 唤醒能力，CommitLog disk-flush 已通过共享 receiver enqueue，CommitLog 已独占 MappedFileQueue/DefaultFlushManager/ConsumeQueue recovery completion，dispatcher worker 只持不可变快照 capability，LocalStore back-reference 已由原子发布的窄 context 替代，transaction 的直接 Store 兼容 owner 已删除；R13 已完成。
4. Store queue：ConsumeQueueExt 与 trait-object handle 已改用每实例显式锁 owner；local/single queue 只持窄标准 Arc 上下文与 Weak lookup，完整 LocalStore carrier 已删除，R12 已完成。
5. Store timer/HA：BrokerStats observer、HA notification service、connection registry 查询、未共享 client/connection child、HA connection worker 自引用、完整 auto-switch weak owner、client 构造 Store capability 与 wrapper 重复 Store owner 已退出多余 owner；RocksDB composition root 已改为独占 Box，Timer 活跃路径已改持队列查询、CommitLog 读、内部消息写三项窄 capability，owned wiring、单测与恢复集成夹具均不再共享完整 LocalStore root；公开 legacy carrier 仅由 R18 next-major/HUMAN 兼容窗口处理。Default/General/AutoSwitch HA service 与 actor 回指已完成。
6. compatibility/stable：production Store `WeakArcMut` 与活跃 test/bench caller 已清零；Remoting/Controller、
   Runtime 与 ArcMut compatibility 的 8 个 nightly gate 全部删除，baseline/target guard 均报告 0 feature，
   stable workspace 矩阵通过。Miri 进一步证明 legacy safe mutable alias 产生 UB，Loom 2/2 固定 guarded
   replacement；R23 技术审计结论为不得长期保留 facade，仍按 next-major/HUMAN 窗口处理其 1/7 自测余量。
7. 候选快照 Gate：R24 已固化同一 commit 的六小时 soak/SLO、dashboard/alerts/runbook/rollback 与 evidence
   index 工程合同；继续执行动态 Kind/K3d/container 与真实 production evidence，并完成
   `[ARCH]`、`[REV]`、`[TEST]`、`[HUMAN]` 签署。

上述 7 个批次是依据当前代码热点形成的执行计划，不是“还剩 7 个正式工作包”。实际 PR 数可因每个切片的风险与审查
大小拆分，但完成目标不能通过合并批次而减少。

## 执行层最小审查清单（31 项）

> 本清单给出当前可执行下界。`identity / occurrence` 来自 Issue #8645 / M11-12bc112 后通过
> `python scripts/arc_mut_guard.py` 验证的 reviewed baseline；production 16 项精确合计 Broker 0/0、Store
> 6/12。test 余量 1/7 均为 ArcMut facade 自身兼容性测试，随 R18 公开兼容窗口关闭。

### Production owner（16 项）

- [x] R01 Broker runtime aggregate：Issue #8635 / bc107 删除最后的私有 `LegacyEscapeStoreOwner<ArcMut<_>>` constructor/type reference；Broker 直接持有标准 `Arc<OwnedMessageStore>`，EscapeBridge/Admin 只保存 Weak provider，请求期只读 lease 与共享 append port 只升级标准 Arc。read-mode/topic-delete/role-sync 使用 Store 原子运行态，生命周期继续通过解绑 provider + `Arc::get_mut` 保持独占；Broker production 从 2/2 降至 0/0。
- [x] R02 Broker Ack 内部 capability：Issue #8519 已删除 `ack_message_processor.rs` 的完整 runtime/revive ArcMut owner（3/5），改持显式 policy/capability，PopRevive task receiver 同步改为标准 Arc。
- [x] R03 Broker send/reply：Issue #8523 已删除 `send_message_processor.rs` 与 `reply_message_processor.rs` 的完整 runtime/ArcMut owner（6/13），改持标准 Arc、热更新 policy、弱 Store 与显式 Topic/订阅/重平衡/统计/reply-channel capability；测试 glob 同步删除 2/2。
- [x] R04 Broker POP：Issue #8531 已删除 `pop_message_processor.rs`、`pop_buffer_merge_service.rs`、`pop_revive_service.rs` 的 8 个 production identities/17 occurrences 与 3 个 test identities/3 occurrences；改持热更新 policy、显式 capability、弱 Store 与父 TaskGroup，`GetMessageResult` 改为独占借用，无 relocation 或新增 identity。
- [x] R05 Broker pull：Issue #8525 已删除 `pull_message_processor.rs` 与 `default_pull_message_result_handler.rs` 的完整 runtime/ArcMut owner（4/7），改持热更新 policy、显式 RPC/Topic/Subscription/Filter/Consumer/Offset/Stats/ColdData/LongPolling capability 与弱 Store provider；测试 glob/ArcMut helper 同步删除 2/4。
- [x] R06 Broker admin config：Issue #8527 已删除 `admin_broker_processor.rs` 与 `broker_config_request_handler.rs` 的 5 个 production identities/7 occurrences 以及 1/1 test glob；admin config 不再直接导入 ArcMut 或调用 `mut_from_ref`，其中 1/1 dispatcher owner 经 ADR-013 一对一迁移为 R01 组合根兼容 carrier。
- [x] R07 Broker offset/failover：Issue #8529 已删除 `consumer_offset_manager.rs` 与 `escape_bridge.rs` 的 4 个 production identities/8 occurrences 以及 1/1 test glob；offset 改持弱 Store 查询 capability，failover 改持热更新 policy、路由/API 和晚绑定 Store capability，其中 1/1 Store owner 经 ADR-013 一对一迁移为 R01 组合根兼容边界。
- [x] R08 Broker pre-online：Issue #8521 已删除 `broker_pre_online_service.rs` 的完整 runtime owner（2/3），改持显式 policy/live role/弱 Store 与 metadata/registration/special-service capability；bc54 同步删除 R01 中无调用方的 runtime start helper 1 occurrence。
- [ ] R09 Store root facade：`message_store.rs`、`base/message_store.rs` 的 concrete alias/unsafe facade（2/5；bc65 已删除零调用方的共享引用可变 CommitLog facade，bc77 删除 `ArcConsumeQueue` 的 unsafe alias 传播，bc80 将 `lib.rs` lifecycle probe 的 shared owner 构造收回 LocalFile owner 模块并删除其 ArcMut import）。
- [x] R10 Store LocalFile root：Issue #8615 已将 Broker、RocksDB、benchmark 与 README 示例全部切换到 `wire_owned_root_dependencies`；CommitLog benchmark 直接独占 concrete LocalFile root，并通过共享引用的 `CommitLog` futures 保持并发锁压测，不再导入、构造、克隆或可变解引用完整 Store wrapper。内部活跃 caller 已清零；剩余 import + `set_message_store_arc` 参数 2/2 仅作为公开 legacy 兼容入口保留并转由 R18 next-major/HUMAN 窗口关闭。
- [x] R11 Store RocksDB root：Issue #8637 / bc108 将 `RocksDBMessageStore.local_file_store` 从 ArcMut 改为独占 Box，删除零调用的 `local_file_store_arc()` 完整 mutable root clone accessor；constructor/import/type propagation 3 identities / 4 occurrences 清零，Local/Rocks borrowed dispatch、recovery 与 lifecycle 行为保持。
- [x] R12 Store queue：Issue #8573 已将 queue facade、consume-queue store、local queue store 与 single queue 的剩余 4 identities / 6 occurrences 清零；bc76 将 ConsumeQueueStore cloneable root 改为标准 Arc，bc77 将 trait-object handle 改为标准 Arc + 每队列 RwLock，bc78 以窄标准 Arc context、CommitLog read handle 与 Weak queue lookup 删除完整 LocalStore compatibility carrier。
- [x] R13 Store WAL/flush：Issue #8553 已将 `commit_log.rs` 的 recovery/LocalStore owner 从 2/7 清零；bc68 使 CommitLog 独占 MappedFileQueue，bc69 使其直接拥有 DefaultFlushManager 与窄 dispatcher handle，bc70 以 `CommitLogStoreContext` 和自有 ConsumeQueue recovery completion 删除完整 LocalStore 回指。
- [x] R14 Store timer：Issue #8611 已让活跃 Timer 路径只持队列查询、CommitLog 读和内部消息写 capability，LocalFile owned wiring、Timer 单测与恢复集成测试不再构造、克隆或可变解引用完整 Store root；公开字段及 `new`/`new_with_config`/setter 的 2 identities / 5 occurrences 仅作为显式 legacy surface 保留，并转由 R18 next-major/HUMAN 兼容窗口关闭。恢复回归同时修复 ConsumeQueue truncate 持读锁再申请同队列写锁的重入死锁。
- [x] R15 Store default HA：Issue #8591 已将 Default HA service/client/connection 的 service/actor ownership 清零；bc75～bc86 完成标准 Arc runtime、replica-store capability、owned connection registry、窄 context/Weak owner 与独占初始化，bc87 删除最后 1/1 import-only composition debt并把重复测试 Store 夹具收口为单一 cfg(test) owner。
- [x] R16 Store general/auto-switch HA：Issue #8591 已将 General 与 AutoSwitch HA 的剩余 3 identities / 6 occurrences 清零；`GeneralHAService` variant 使用标准 `Arc`，group-transfer/state-notification 通过晚绑定标准 `Weak` 回指，AutoSwitch 继续唯一拥有 Default delegate，根释放后 child 无法恢复 owner。

### Caller 与 compatibility（2 项）

- [x] R17 Test/bench caller 迁移：Issue #8639 / bc109 将 RocksDB semantics helper 从 `GenericMessageStore` + `ArcMut` 改为独占 `OwnedMessageStore`，并将 16 个 Broker Store-capability 测试模块的 type-only facade 参数一并迁移；Store/Broker/Client 活跃 test/bench caller 全部清零，baseline test 从 3/9 降至 2/8。bc112 又删除过时 comparison benchmark 与随 stable backing 消失的 test glob debt，当前 test 为 1/7；余量均是 `rocketmq/src/arc_mut.rs` 对公开 facade 自身的兼容性测试，转由 R18 next-major/HUMAN 窗口关闭。
- [ ] R18 Public compatibility 删除：bc112 已把 backing 稳定化，但没有删除公开名称；在 next-major 两轮弃用和 Release Manager/HUMAN 批准后，删除 `rocketmq/src/arc_mut.rs` 与 `rocketmq/src/lib.rs` 的剩余 13 identities / 39 occurrences，并关闭从 R09～R11/R14 转入的 Store 公开 legacy carrier；不得以重置 public API baseline 代替迁移。

### M10 / Phase 3 候选快照 Gate（7 项）

- [ ] R19 M10 固定硬件 baseline/candidate、正确性优先性能 Gate 与 HUMAN 签署。
- [ ] R20 M11 五服务镜像动态构建/启动、non-root/read-only、SBOM、签名与漏洞策略验证。
- [ ] R21 M11 Kind/K3d 七类 fault/rolling 场景和持久化证据验证。
- [x] R22 同一冻结 commit 的 stable default、完整 feature matrix 与 nightly surface 删除验证。Issue #8641 /
  bc110 保留 `RequestProcessorV2::Fut` 公开 GAT API，将 core built-in 改为 concrete `Ready` future、测试/
  example 改为显式 boxed future，删除 Remoting 三处 `impl_trait_in_assoc_type` 与 Controller
  `arbitrary_self_types`；新增 baseline/target stable-surface guard，将 feature 属性从 8 个降至 4 个。
  Issue #8643 / bc111 又以 owned-future 兼容转发层删除 Runtime `async_fn_traits`/`unboxed_closures`，Runtime
  stable all-target 通过，feature 属性降至 2 个。Issue #8645 / bc112 以 stable `parking_lot::RwLock`
  backing 保留 ArcMut 兼容名称，删除最后两处 gate 与过时 benchmark；baseline/target guard 报告 0 feature，
  stable default 与 workspace all-target/all-feature 矩阵在同一候选快照通过。
- [x] R23 Miri/Loom 可用切片、soundness proof 与保留 wrapper ADR 审核。Issue #8647 / bc113 新增隔离生成的 Miri runner：guarded `RwLock` backing 通过，两个 ArcMut clone 仅以 safe `DerefMut` 创建的可变别名被判定为预期 UB；分类器拒绝意外 PASS 和无关工具失败。Loom 2/2 验证 `Arc<RwLock<T>>` writer serialization 与 worker owner 释放；0.7.2 不提供 `Weak`，因此未对该交错作超范围声明。审核结论是不批准长期保留 wrapper，R18 必须在既定兼容窗口删除 facade。
- [x] R24 Soak/SLO fault、dashboard、alert、runbook、rollback 与 evidence index 对齐。Issue #8649 /
  bc114 新增六小时/一分钟采样与 1% 缺样上限、七项目标、semantic-registry 对齐的 Grafana/Prometheus
  资产、英文处置/回滚 runbook、真实 fault + Prometheus 动态 runner/workflow 和 fail-closed evidence guard；
  9 项正向/故意违规测试通过。fixture 明确非动态，真实集群执行继续计入 R20/R21，四方签署继续计入 R25。
- [ ] R25 冻结 Phase 3 候选快照，完成 `[ARCH]`、`[REV]`、`[TEST]`、`[HUMAN]` 签署。

### Phase 4 / M12（6 项）

- [ ] R26 / PR-M12-01：Evidence normalization 与 Knowledge Graph。
- [ ] R27 / PR-M12-02：受控 RAG。
- [ ] R28 / PR-M12-03：多领域确定性诊断。
- [ ] R29 / PR-M12-04：冻结 Plan contract 并证明无副作用。
- [ ] R30 / PR-M12-05：仅在 HUMAN 单独批准后实现独立 Apply；若拒绝实施，以签署的 no-Apply 决策关闭条件分支。
- [ ] R31 / PR-M12-06：Eval、red-team 与离线 fallback，并关闭 Phase 4 Gate。

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

M11-12bc25 将 `CommitRealTimeService` 对完整 `DefaultFlushManager` 的 `WeakArcMut` 回指收窄为仅含
group-commit/flush-realtime `Notify` 与 timed policy 的 `FlushWakeup`；`CommitLog::start` 不再 downgrade 或晚绑定
manager，sync/async timed policy 与原 TaskGroup 生命周期保持不变。production 净删除 2 identities / 4 occurrences，
因此 reviewed 总量降至 344/932、production 降至 186/454、Store 降至 101/270；test 144/438、Broker 85/184
与 compatibility 14/40 不增。2 个保留 import occurrence 经一对一指纹审核更新，无新增 identity。

M11-12bc26 将 HA confirm offset 从普通 `i64` 改为 `AtomicI64` 共享发布，并为已有原子 epoch start offset 与
state-machine version 增加窄发布入口；Auto-switch HA service 与 HA reader 不再通过 `mut_from_ref` 取得完整
`LocalFileMessageStore` 可变引用。confirm offset 下降、reader clamp 与 Advanced epoch 发布顺序保持不变。
production 净删除 2 identities / 6 occurrences，因此 reviewed 总量降至 342/926、production 降至 184/448、
Store 降至 99/264；test 144/438、Broker 85/184 与 compatibility 14/40 不增，无 relocation。

M11-12bc27 将 `NotifyBrokerRoleChangeHandler` 改为无状态、非泛型 leaf，经既有
`BrokerConfigRequestHandler` owner 的窄委托调用 `apply_controller_role_change`，不再长期保留第二份完整 runtime
owner。header/body、remote controller address、未初始化 Success、`SystemError` 映射与角色切换顺序保持不变。
production 净删除 2 identities / 3 occurrences，因此 reviewed 总量降至 340/923、production 降至 182/445、
Broker 降至 83/181；test 144/438、Store 99/264 与 compatibility 14/40 不增，无 relocation。

M11-12bc28 删除 `HAConnection::start` 传播的 `WeakArcMut<GeneralHAConnection>` 自引用，read/write worker 改持
connection id、remote address、共享状态和可选 slave broker id 组成的窄 runtime handle；Default/AutoSwitch HA
service 的 ack、caught-up、removed callback 只消费该 handle 或标量能力。状态通知、connection table 清理、confirm
offset 与 sync-state 更新时间、shutdown/TaskGroup 顺序保持不变。production 净删除 8 identities / 13 occurrences，
test 净删除 1 identity / 3 occurrences，因此 reviewed 总量降至 331/907、production 降至 174/432、test 降至
143/435、Store 降至 91/251；Broker 83/181 与 compatibility 14/40 不增，1 个保留 import occurrence 经同位置
指纹审核更新，无 relocation。

M11-12bc29 将 sync-flush enqueue 的真实只读边界显式化：`GroupCommitService::put_request` 改为 `&self`，
`DefaultFlushManager` 以 crate-private shared 方法承载原实现，公开 `FlushManager` 的 `&mut self` 签名继续作为兼容
facade；CommitLog 直接调用 shared 方法并删除唯一 `mut_from_ref`。cancellation、bounded channel backpressure、
原子 enqueue stats、receiver timeout、状态映射与独占 start/shutdown lifecycle 保持不变。production 净删除
1 identity / 1 occurrence，因此 reviewed 总量降至 330/906、production 降至 173/431、Store 降至 90/250；
test 143/435、Broker 83/181 与 compatibility 14/40 不增，无 relocation。

M11-12bc30 将 `AutoSwitchHAService::replication` 改为标准 `Arc<ReplicationStateRoot>`，由
`DefaultHAService` 只保存该窄状态能力；connection added/ack/caught-up/removed 直接通过状态根与 delegate 已有 Store
能力更新 caught-up 时间、sync-state set 和 confirm offset，不再保存、升级或降级完整 auto-switch service。
shutdown 短路、sync-state expansion/removal、confirm-offset runtime snapshot 和发布顺序保持不变；回归证明初始化后完整
auto-switch owner 的 weak count 为零。production 净删除 2 identities / 7 occurrences，因此 reviewed 总量降至
328/899、production 降至 171/424、Store 降至 88/243；test 143/435、Broker 83/181 与 compatibility 14/40
不增。4 个保留 occurrence 经临时 ADR-013 一对一 relocation 审核，无新增 identity，审批文件不提交。

M11-12bc31 将 `TopicRequestHandler` 改为无状态、非泛型 leaf；Admin dispatch 为 topic 查询/clean 传入请求期共享
`BrokerRuntimeInner` 借用，为删除传入请求期独占借用，create/update 则复用已有 `BrokerConfigRequestHandler` owner
执行 coordinator persist 与 single/increment registration。Topic validation、static mapping、Mixed/system 限制、
idempotency、删除 POP retry v2/v1/main 顺序、offset/inflight/Store 清理和查询响应保持不变；零大小回归证明 handler
不再保活完整 runtime。production 净删除 2 identities / 3 occurrences，test 净删除 1 identity / 1 occurrence，因此
reviewed 总量降至 325/895、production 降至 169/421、test 降至 142/434、Broker 降至 81/178；Store 88/243 与
compatibility 14/40 不增，无 relocation。

M11-12bc32 将 `AutoSwitchHAClient` 的完整 Store 构造能力移出 wrapper：service 先以现有
`ArcMut<LocalFileMessageStore>` 构造 `DefaultHAClient`，保留原 `HAClientError` 到 `HAError::Service` 映射，再经
crate-private `from_delegate` 包装和安装。delegate 报告 broker ID、wrapper 原子 broker ID、master address、运行状态与
初始化顺序保持不变；测试显式构造 delegate 后验证包装语义。production 净删除 4 identities / 4 occurrences，test 净删除
1 identity / 1 occurrence，因此 reviewed 总量降至 320/890、production 降至 165/417、test 降至 141/433、Store
降至 84/239；Broker 81/178 与 compatibility 14/40 不增。4 个保留 test occurrence 经临时 ADR-013 一对一
relocation 审核，无新增 identity，审批文件不提交。

M11-12bc33 将 `QueryAssignmentProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为启动配置快照、可刷新
`TopicRouteInfoManager` 和只暴露 client-id 列表的 live `ConsumerAssignmentView`；processor 与 dispatch variant 同时退出
MessageStore 泛型。请求模式 load/persist、NameServer route refresh、主 consumer table 变化可见性与启动期默认参数语义保持
不变，动态配置回归锁定四个默认参数不在运行期 allowlist。production 净删除 2 identities / 3 occurrences，test 净删除
1 identity / 1 occurrence，因此 reviewed 总量降至 317/886、production 降至 163/414、test 降至 140/432、Broker
降至 79/175；Store 84/239 与 compatibility 14/40 不增。1 个相邻保留 production occurrence 经临时 ADR-013
一对一 relocation 审核，无新增 identity，审批文件不提交。

M11-12bc34 将 `AutoSwitchHAService` 的构造输入改为 owned `DefaultHAService` delegate，删除 wrapper 重复保存的
完整 `ArcMut<LocalFileMessageStore>` field；初始角色和所有 Store 读/发布操作均经 delegate 只读访问，AutoSwitch client
通过 delegate 的 crate-private factory 构造并保留原错误映射与初始化顺序。强引用回归证明构造只增加 delegate 所需的一份
Store owner。production 净删除 2 identities / 5 occurrences，因此 reviewed 总量降至 315/881、production 降至
161/409、Store 降至 82/234；test 140/432、Broker 79/175 与 compatibility 14/40 不增。18 个保留 occurrence
经临时 ADR-013 一对一 relocation 审核，无新增 identity，审批文件不提交。

M11-12bc35 将 `PollingInfoProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为启动配置、共享 Topic manager、
只暴露 find 的 live SubscriptionGroup lookup 与弱 polling-count provider，并移除 MessageStore 泛型；POP service 释放后
轮询查询回落为 0。`SubscriptionGroupManager` 同时删除完整 Runtime owner 与泛型，改持显式配置快照和 Store 只读
`StateMachineVersionView`，保留 auto-create、DataVersion、JSON/RocksDB persist 与实时 WAL flush 语义。production
净删除 4 identities / 7 occurrences，test 净删除 1/1，因此 reviewed 总量降至 310/873、production 降至
157/402、test 降至 139/431、Broker 降至 75/168；Store 82/234 与 compatibility 14/40 不增。无 relocation、
新增 identity 或临时 approval。

M11-12bc36 将 `TopicQueueMappingCleanService` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为 broker name、forward
timeout、delete window 启动期快照，共享 `TopicQueueMappingManager`、可克隆 `BrokerOuterAPI` 与可选父 TaskGroup，并移除
MessageStore 泛型。定时任务继续优先挂在 Broker service TaskGroup 下，无 context 时保留 ambient Tokio fallback；expired
item/old generation 清理、持久化、幂等启动与 shutdown report 语义保持不变。production 净删除 2 identities / 3
occurrences，test 净删除 5/7，因此 reviewed 总量降至 303/863、production 降至 155/399、test 降至 134/424、
Broker 降至 73/165；Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc37 将 `ClientManageProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为启动期 `BrokerConfig`、共享
`TopicConfigManager`、live SubscriptionGroup lookup、Producer/Consumer registration handle 与显式 retry-topic
registration capability。heartbeat v1/v2、unregister、filter validation、重试主题队列数/order/sys-flag、持久化和
NameServer registration 语义保持不变；显式 Store 兼容 owner 仍计入未完成债务。production 净删除 2 identities / 3
occurrences，test 净删除 1/1，因此 reviewed 总量降至 300/859、production 降至 153/396、test 降至 133/423、
Broker 降至 71/162；Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc38 将 `ConsumerManageProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为 live consumer-id view、
`ConsumerOffsetRequestCapability`、Topic/Subscription/mapping manager、共享 RPC client 与两个启动期配置标量。offset
capability 复用 `ConsumerOffsetManager` 既有 owner，不新增 Store/ArcMut 强引用；consumer list、local/static-topic
offset update/query、Store fallback 与 RPC error mapping 语义保持不变。production 净删除 2 identities / 3
occurrences，因此 reviewed 总量降至 298/856、production 降至 151/393、Broker 降至 69/159；test 133/423、
Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc44 联合收窄 `NotificationProcessor` 与 `PopLongPollingService`：前者改为启动 policy、共享
Topic/Subscription/Order 能力和弱 offset/Store/POP provider；后者删除 MessageStore 泛型与完整 runtime owner，改持
容量策略、Topic/Subscription 查询和可选父 `TaskGroup`。扫描、清理、容量、超时与 wake-up 生命周期保持 owned，provider
退出时按既有 0、-1 或无消息语义 fail closed。production 净删除 4 identities / 6 occurrences，因此 reviewed 总量
降至 281/832、production 降至 137/372、Broker 降至 55/138；test 130/420、Store 82/234 与 compatibility
14/40 不增。1 个相邻保留 occurrence 经 ADR-013 一对一 relocation 审核，无新增 identity 或临时 approval 提交。

M11-12bc45 收窄 `ChangeInvisibleTimeProcessor`：删除完整 runtime owner 与 `Arc<PopMessageProcessor>` 间接 owner，
改持启动 policy、共享 Topic/Stats、弱 consumer offset/order/Store/POP provider 和独立 queue lock。普通 POP 与顺序 POP
的 offset 校验、revive checkpoint/ack、统计、锁和响应语义在 provider 存活时保持不变；provider 退出时返回
`ServiceNotAvailable` 或按原 fallback fail closed。production 净删除 2 identities / 3 occurrences，因此 reviewed
总量降至 279/829、production 降至 135/369、Broker 降至 53/135；test 130/420、Store 82/234 与 compatibility
14/40 不增。1 个保留的外层 processor wrapper 经 ADR-013 一对一 relocation 审核，无新增 identity 或临时 approval 提交。

M11-12bc46 收窄 `PopLiteLongPollingService`：删除完整 runtime owner 与 `MessageStore` 泛型，改持启动期容量
policy、可克隆 `LiteEventDispatcher` 和显式父 `TaskGroup`；组合根负责从 runtime 提取这些能力。polling map 容量、
全局/客户端限流、过期扫描、事件 wake-up、幂等启动和有界 shutdown 语义保持不变。production 净删除 2 identities /
3 occurrences，因此 reviewed 总量降至 277/826、production 降至 133/366、Broker 降至 51/132；test 130/420、
Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc47 收窄 `PopLiteMessageProcessor`：删除完整 runtime owner，改持启动 policy、共享 Topic/Subscription 查询、
弱 consumer-offset/Store provider、Lite dispatcher、独立 queue lock 和已收窄 long-polling context；Broker 组合根负责
提取所有能力。provider 退出后按无消息、offset 缺失或 no-op commit fail closed，校验、LMQ 读取、offset 校正、顺序
消费、事件重排和 polling 语义保持不变。production 净删除 2 identities / 5 occurrences，测试中的
`LocalFileMessageStore` 传播额外净删除 2/2，因此 reviewed 总量降至 273/819、production 降至 131/361、test 降至
128/418、Broker 降至 49/127；Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc48 收窄 `LiteSubscriptionCtlProcessor`：删除完整 runtime owner，改持容量 policy、共享 registry/dispatcher/
group view、弱 consumer-offset/Store provider 与弱 POP Lite order-info provider。provider 退出后 query/max-offset/reset/
order clear fail closed，partial/complete add/remove、exclusive、quota 与 offset reset 语义保持不变。`LiteManager` 与
`LiteSubscriptionCtl` 的外层 wrapper 改为标准 Arc，六个 LiteManager 路由共享单一 processor。production 净删除
2 identities / 12 occurrences，测试 glob/ArcMut helper 额外净删除 2/2，因此 reviewed 总量降至 269/805、production
降至 129/349、test 降至 126/416、Broker 降至 47/115；Store 82/234 与 compatibility 14/40 不增。无 relocation、
新增 identity 或临时 approval。

M11-12bc49 收窄 `LiteManagerProcessor`：删除完整 runtime owner，改持启动 policy、显式 TopicConfig/
SubscriptionGroup/LiteSubscription/LiteEvent/LiteLifecycle 与 sharding route view，以及弱 consumer-offset/Store/
POP-order provider。Lite lag calculator 和 sharding helper 同步删除完整 runtime 参数；provider 退出后 offset/store/
order 查询 fail closed，六个 Lite manager 请求的校验、sharding、lag/offset 与 dispatch 语义保持不变。production
净删除 2 identities / 3 occurrences，因此 reviewed 总量降至 267/802、production 降至 127/346、Broker 降至
45/112；test 126/416、Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc50 收窄 `SlaveSynchronize`：删除完整 runtime owner、构造传播与 subscription-group `mut_from_ref`，改持
broker/timer policy、BrokerOuterAPI、弱 TopicConfig/coordinator/mapping/Schedule/Timer provider、晚绑定弱
ConsumerOffsetManager 与晚绑定 MessageRequestModeManager。shutdown 在 metadata/Store detach 前释放仍可能强保活的
subscription/request-mode capability，RocksDB migration/recovery 保持通过；所有 provider 退出均 fail closed。production
净删除 3 identities / 4 occurrences，因此 reviewed 总量降至 264/798、production 降至 124/342、Broker 降至
42/108；test 126/416、Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc51 删除 `rocketmq-broker` crate-wide `#![allow(clippy::mut_from_ref)]`。Broker 与 root workspace
all-target/all-feature strict Clippy 在无豁免时通过，证明当前 Broker 不需要全局压制该 lint；后续实现若重新暴露从共享引用
取得可变引用，将由默认 lint 直接阻止。production 净删除 1 identity / 1 occurrence，因此 reviewed 总量降至
263/797、production 降至 123/341、Broker 降至 41/107；test 126/416、Store 82/234 与 compatibility 14/40 不增。
无 relocation、新增 identity 或临时 approval。

M11-12bc52 将 Ack、ChangeInvisibleTime 与 AdminBroker 三个遗留 processor registry wrapper 退出 ArcMut。Ack 与
ChangeInvisibleTime 使用标准 Arc 和共享请求入口；Ack start/status/shutdown 只通过既有 atomic 与 TaskGroup 锁更新状态；
AdminBroker 的真实配置 mutation 通过 `Arc<tokio::sync::Mutex<_>>` 显式串行化，不再从共享 wrapper 制造并发可变引用。
BrokerRuntime 的 Ack handle、构造和 unchecked accessor 同步改为标准 Arc。production 净删除 2 identities / 9
occurrences，test glob 额外净删除 1/1，因此 reviewed 总量降至 260/787、production 降至 121/332、test 降至
125/415、Broker 降至 39/98；Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc39 将 `QueryMessageProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为默认查询上限与
`QueryMessageStoreCapability`。capability 复用既有 `Weak<EscapeBridge>` Store provider，只暴露 Store availability、索引查询与
按物理 offset 读取，不新增或转移 `ArcMut` owner，也不强保活 runtime；QueryMessage/ViewMessageById 的响应码、remark、响应 body 与索引安全
语义保持不变。production 净删除 2 identities / 3 occurrences，test 通配导入债务额外净删除 1/1，因此 reviewed 总量
降至 295/852、production 降至 149/390、test 降至 132/422、Broker 降至 67/156；Store 82/234 与 compatibility
14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc40 将 `RecallMessageProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为 `RecallMessagePolicy`、
共享 Topic/Stats handle 与 `RecallMessageStoreCapability`。Store capability 只持 `Weak<EscapeBridge>`，请求期读取 live
Broker role 并执行直接本地 Store put；controller role change 不会被冻结，provider/Store 退出时 fail closed。recall 校验、
tombstone properties、put-result 与统计语义保持不变。production 净删除 2 identities / 3 occurrences，test 通配导入
债务额外净删除 1/1，因此 reviewed 总量降至 292/848、production 降至 147/387、test 降至 131/421、Broker 降至
65/153；Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc41 将 `EndTransactionProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为
`EndTransactionPolicy`、共享 BrokerStats handle 与 `EndTransactionStoreCapability`。Store capability 只持
`Weak<EscapeBridge>`，请求期读取 live Broker role 并执行直接本地 Store put；provider/Store 退出时返回稳定的
`ServiceNotAvailable`，不再触发生产 `unwrap`。事务校验、prepare deletion、put-result、统计与指标语义保持不变。
production 净删除 2 identities / 3 occurrences，test 通配导入债务额外净删除 1/1，因此 reviewed 总量降至
289/844、production 降至 145/384、test 降至 130/420、Broker 降至 63/150；Store 82/234 与 compatibility
14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc42 将 `TransactionMessageStore` 从直接 `ArcMut<MS>` owner 改为 `Weak<EscapeBridge>` Store provider，
并将 `TransactionalMessageBridge` 的 escape path 从强 Arc 同步改为弱 provider。half/op read、本地 put、state-machine
version、topic generation、HA master-address 更新与 escape 在 provider 存活时保持不变；provider/Store 退出后按操作
返回无数据、`ServiceNotAvailable`、跳过创建/更新或 false，且 transaction 后台组件不再强保活 runtime/Store。
production 净删除 2 identities / 3 occurrences，因此 reviewed 总量降至 287/841、production 降至 143/381、
Broker 降至 61/147；test 130/420、Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时
approval。

M11-12bc43 将 `PeekMessageProcessor` 从完整 `ArcMut<BrokerRuntimeInner>` owner 收窄为启动 policy、共享
Topic/Subscription/Offset/Stats 能力，以及弱 EscapeBridge Store provider 和弱 POP merge-offset provider。消费位点
查询使用不强保活 `ConsumerOffsetManager`/Store 的 weak capability；Store/POP/offset provider 退出时按既有 0、-1 或
无消息语义 fail closed，权限、重试主题、位点校正、消息读取与统计语义保持不变。production 净删除 2 identities / 3
occurrences，因此 reviewed 总量降至 285/838、production 降至 141/378、Broker 降至 59/144；test 130/420、
Store 82/234 与 compatibility 14/40 不增。无 relocation、新增 identity 或临时 approval。

M11-12bc54 收窄 Broker pre-online 边界：service 删除完整 `BrokerRuntimeInner` owner，改持不可变启动 policy、实时
broker role、弱 Store/HA 与 metadata provider、registration capability 和显式 special-service capability。controller
角色切换同步发布 live role，Store/provider 退出时 fail closed；service 在依赖就绪后构造，后台任务挂载 Broker 父
`TaskGroup`，并在 Store/metadata detach 前停止。transaction check service 改为标准 Arc 以提供弱 capability；无调用方的
runtime start helper 同步删除。production 净删除 2 identities / 4 occurrences，因此 reviewed 总量降至 255/775、
production 降至 116/320、Broker 降至 34/86；test 125/415、Store 82/234 与 compatibility 14/40 不增。3 个保留
occurrence 完成一对一指纹审核，无新增 identity 或临时 approval。R08 完成，31 项执行清单已完成 2 项、剩余 29 项。

M11-12bc55 收窄 Broker send/reply 边界：两个 processor 及共享 `Inner` 删除完整 `BrokerRuntimeInner` 与
`ArcMut` owner，改持标准 Arc、不可变 hook 集合、热更新 policy、弱 Store provider、显式 Topic/Subscription/
Rebalance/Stats 与 producer reply-channel capability。send append 保留 typed Store error，provider 退出时返回
NotStarted；reply 保持 push-first 顺序并在 Store 缺失时 fail closed。旧 send-topic 宏由显式 Topic 创建、持久化和
注册边界替代。production 净删除 6 identities / 13 occurrences，test glob 同步删除 2/2，因此 reviewed 总量降至
247/760、production 降至 110/307、test 降至 123/413、Broker 降至 28/73；Store 82/234 与 compatibility
14/40 不增。1 个保留 BrokerRuntime root constructor 经临时 ADR-013 一对一 relocation 审核，无新增 identity 或
提交态临时 approval。R03 完成，31 项执行清单已完成 3 项、剩余 28 项。

M11-12bc56 收窄 Broker pull 边界：`PullMessageProcessor` 与 `DefaultPullMessageResultHandler` 删除完整
`BrokerRuntimeInner`/`ArcMut` owner，改持基于 `ArcSwap` 的热更新 pull policy、显式 RPC/Topic/Subscription/Filter/
Consumer/Offset/Stats/ColdData/LongPolling capability 和弱 `EscapeBridge` Store provider。组合根一次安装 long-polling
service，保持 processor 弱回边；Store/provider 退出时 offset/read 路径返回协议级 `SystemError` 或无 offset，替代原有
`unwrap`。PullMessage/LitePullMessage 的校验、转发、过滤、静态 Topic、冷数据、offset、统计、挂起与唤醒语义保持不变。
production 净删除 4 identities / 7 occurrences，test glob/ArcMut helper 同步删除 2/4，因此 reviewed 总量降至
241/749、production 降至 106/300、test 降至 121/409、Broker 降至 24/66；Store 82/234 与 compatibility
14/40 不增。1 个保留 BrokerRuntime root constructor 经临时 ADR-013 一对一 relocation 审核，无新增 identity 或
提交态临时 approval。R05 完成，31 项执行清单已完成 4 项、剩余 27 项。

M11-12bc57 收窄 Broker admin config 边界：`AdminBrokerProcessor` 与 `BrokerConfigRequestHandler` 不再直接
导入或传播 `ArcMut<BrokerRuntimeInner>`，配置更新与 commit-log read mode 也不再从共享引用调用 `mut_from_ref`。
兼容 owner 以 `BrokerAdminRuntimeHandle` 明确收回 R01 组合根，admin Tokio mutex 下的共享/独占请求借用、Topic
持久化/注册和 controller role-change 顺序保持不变。原 R06 的 5 个 production identities/7 occurrences 与 1/1
test glob 全部删除，其中 dispatcher 的 1/1 owner 经临时 ADR-013 一对一迁移为 R01 carrier；因此 production 净减少
4/6、test 净减少 1/1，reviewed 总量降至 236/742、production 降至 102/294、test 降至 120/408、Broker 降至
20/60；Store 82/234 与 compatibility 14/40 不增。临时 approval 不提交。R06 完成，R01 精确余量调整为 8/35，
31 项执行清单已完成 5 项、剩余 26 项。

M11-12bc58 收窄 Broker offset/failover 边界：`ConsumerOffsetManager` 删除直接 Store ArcMut owner，改由弱
`EscapeBridge` capability 执行请求 fallback、state-version 与未订阅 Topic 清理查询；`EscapeBridge` 删除完整
`BrokerRuntimeInner` owner，改持热更新 failover policy、共享 route/API 和晚绑定 Store capability。本组原 4 个
production identities/8 occurrences 与 1/1 test glob 全部删除，其中原 failover Store owner 的 1/1 经临时 ADR-013
从 2 occurrences 压缩并一对一迁移为 R01 兼容 owner；production 净减少 3/7、test 净减少 1/1，reviewed 总量
降至 232/734、production 降至 99/287、test 降至 119/407、Broker 降至 17/53；Store 82/234 与 compatibility
14/40 不增。临时 approval 不提交。R07 完成，R01 精确余量调整为 9/36，31 项执行清单已完成 6 项、剩余 25 项。

M11-12bc59 收窄 Broker POP 边界：`PopMessageProcessor`、`PopBufferMergeService` 与 `PopReviveService`
删除完整 `BrokerRuntimeInner`、ArcMut 和 `mut_from_ref`，改持热更新 policy、显式 capability、弱 Store provider
与父 `TaskGroup`；请求聚合从共享可变 `GetMessageResult` 改为栈上独占值和串行 `&mut` 借用。原 R04 的 8 个
production identities/17 occurrences 与 3 个 test identities/3 occurrences 全部删除，无 relocation、新增
identity 或临时 approval；reviewed 总量降至 221/714、production 降至 91/270、test 降至 116/404、Broker
降至 9/36，Store 82/234 与 compatibility 14/40 不增。R04 完成，31 项执行清单已完成 7 项、剩余 24 项。

M11-12bc60 继续收窄 R01 Admin/control-plane 组合根：删除 `BrokerAdminRuntimeHandle` 及其完整
`ArcMut<BrokerRuntimeInner>` owner，Admin dispatcher 和全部 leaf 改持显式 `BrokerAdminRuntime`；broker/store 配置
通过单一 `ArcSwap` 代际原子发布，controller、membership、role transition 与 Store HA 操作改由显式状态和 capability
组合。Admin 上下文不再保活或解引用 runtime root，producer 共享视图可观察后续配置代际。reviewed baseline 从
221/714 降至 220/713，production 从 91/270 降至 90/269，Broker 从 9/36 降至 8/35；test 116/404、Store
82/234 与 compatibility 14/40 不增。16 个保留 occurrence 经临时 ADR-013 一对一指纹审核，无新增 identity，
approval 不提交。R01 尚未完成，31 项执行清单仍为已完成 7 项、剩余 24 项；下一切片 M11-12bc61 继续启动、注册、
后台任务或 Local/Rocks 组合根 owner。

M11-12bc61 将 controller bootstrap、leader discovery、broker ID、heartbeat、replica metadata 与 membership 周期迁入
显式 `BrokerControllerRuntime`；reviewed baseline 从 220/713 降至 220/703，production 从 90/269 降至 90/259，
Broker 从 8/35 降至 8/25。M11-12bc62 又将 NameServer 注册迁入 `BrokerRegistrationRuntime`，state observer 改持
共享 manager；baseline 降至 220/697、production 90/253、Broker 8/19。两切片均无 relocation、新增 identity 或
临时 approval。

M11-12bc63 继续收窄 R01 background/observation 边界：observability、scheduled maintenance 与 metadata refresh 的
20 处 production `self.inner.clone()` 全部改为 Topic/Client/POP/Store/Timer/Slave/API 等窄能力捕获；Store、RocksDB
与 tiered-store 指标复用既有晚绑定 capability，不新增 Store owner。MessageStore accessor 从 `ArcMut<MS>` 返回类型
收窄为 `&MS`/`&mut MS`，并删除无调用方的 unchecked shared accessor。reviewed baseline 从 220/697 精确降至
220/693，production 从 90/253 降至 90/249，Broker 从 8/19 降至 8/15；test 116/404、Store 82/234 与 compatibility
14/40 不增，无 relocation、新增 identity 或临时 approval。R01 尚未完成，执行清单仍为已完成 7 项、剩余 24 项；
下一切片 M11-12bc64 迁移 test caller 并将 `BrokerRuntime.inner` 改为独占值。

M11-12bc64 随 Issue #8541 将 `BrokerRuntime.inner` 从共享 `ArcMut<BrokerRuntimeInner>` 改为独占
`Box<BrokerRuntimeInner>`，并把 43 处 `inner_for_test().clone()` 迁移为短借用、owned `BrokerAdminRuntime`、共享
Producer/Consumer manager、Topic manager、Lite registry 与 ConsumerOffset handle。生命周期测试不再以 root
`strong_count` 模拟所有权，而由 production source contract 证明完整 root 不可共享；Store 自身必要的强引用合同保持。
reviewed baseline 从 220/693 精确降至 220/690，production 从 90/249 降至 90/246，Broker 从 8/15 降至
8/12；test 116/404、Store 82/234 与 compatibility 14/40 不增，无 relocation、新增 identity 或临时 approval。
R01 尚未完成，剩余 12 个 Broker occurrence 均为 Local/Rocks/Store owner carrier；执行清单仍为已完成 7 项、
剩余 24 项，正式进度仍为 75/82。

M11-12bc65 随 Issue #8543 删除全仓零真实调用方的 `MessageStore::get_commit_log_mut_from_ref`，以及
Generic、LocalFile 与 RocksDB 三层 forwarding/实现；普通共享 `get_commit_log` 与独占
`get_commit_log_mut(&mut self)` 保持。源码合同禁止四个边界文件重新引入该 `&self -> &mut CommitLog` facade。
由于 LocalFile/RocksDB 不再从包含 `ArcMut` 字段的 aggregate 导出共享引用可变借用，guard 的传递闭包同步退出
Local/Rocks concrete wrapper：reviewed baseline 从 220/690 精确降至 138/506，production 从 90/246 降至
62/168，test 从 116/404 降至 62/298，compatibility 保持 14/40；Broker production 从 8/12 降至 4/8，
Store 从 82/234 降至 58/160。净删除 82 identities/184 occurrences，无 relocation、新增 identity 或临时
approval。R01、R09～R16 均仍未完成，执行清单仍为已完成 7 项、剩余 24 项，正式进度仍为 75/82。

M11-12bc66 随 Issue #8545 删除 Local-owned background work 对完整 Store root 的直接 owner：Reput
reader/dispatcher、one-shot 与缓存 inner 改持 CommitLog、dispatcher、不可变 delay snapshot、Stats 与 live
message-arrival capability；listener 通过标准 `RwLock` 发布且回调不持锁，shutdown join 后清空 inner。scheduled
self-check 只捕获 CommitLog 与 ConsumeQueueStore 窄 child handle。reviewed baseline 从 138/506 精确降至
138/501，production 从 62/168 降至 62/164，test 从 62/298 降至 62/297，compatibility 保持 14/40；Store
production 从 58/160 降至 58/156，R10 从 6/38 降至 6/34。净删除 4 个 production occurrences 与 1 个 test
occurrence；7 个保留 CommitLog/dispatcher occurrence 经临时 ADR-013 一对一指纹审核，无新增 identity 或提交态
approval。Local root、CommitLog/CQ、Timer、HA 与 recovery 回指仍在，执行清单仍为已完成 7 项、剩余 24 项。

M11-12bc67 随 Issue #8547 完成 CommitLog cleanup 的共享引用安全边界：`CleanCommitLogService`、
`CleanConsumeQueueService` 与 `CorrectLogicOffsetService` 不再持有 `ArcMut<CommitLog>`，只持标准 Arc 驱动的
`CommitLogCleanupHandle`；scheduled cleanup/min-offset 路径因此不再解引用 legacy CommitLog root。增量 create/delete
通过 ArcSwap RCU 在最新 generation 合并，load/recovery/shutdown 的 authoritative replacement 保持 `&mut` 独占
lifecycle 并明确 writer-quiesced 前提。WAL pin、批量上限、delete interval、manual retry budget、固定延迟调度和
shutdown owner 均保持不变。scanner 同步修复 `#[cfg(test)]` 后比较表达式/泛型角括号的范围问题，跨 identity
relocation 比较新增 `remove_by`，禁止借 relocation 延后清理截止期；guard 增至 78 个治理测试。reviewed baseline
从 138/501 降至 137/493；production 为 62/159、test 61/294、compatibility 14/40、Store production 58/151，
相对 bc66 净删除 1 个 identity/8 occurrences。一个相邻 CommitLog constructor fingerprint 经 ADR-013 临时一对一
审核；approval 仅在忽略的 `target/` 中用于 candidate review，未提交到仓库。R10、R13 仍未完成，执行清单仍剩余 24 项。

M11-12bc68 随 Issue #8549 完成 mapped-file flush 的共享引用安全边界：CommitLog 直接拥有
`MappedFileQueue`，`DefaultFlushManager` 及其四个 flush/commit worker 改持只暴露 lookup、commit、flush 与进度读取的
`MappedFileQueueFlushHandle`。handle 克隆标准 Arc mapped-file generation 和共享 runtime state；runtime state clone
共享原子进度与串行锁，不复制 flush/commit 水位。`DefaultFlushManager::new` 收窄为 crate 内构造入口，外部不能重新注入
完整 queue owner。reviewed baseline 从 137/493 降至 133/476；production 从 62/159 降至 60/149，test 从
61/294 降至 59/287，compatibility 保持 14/40；Store production 从 58/151 降至 56/141。相对 bc67
净删除 4 identities/17 occurrences（production 2/10、test 2/7），无 relocation、新增 identity 或临时 approval。
R13 从 5/21 降至 3/11，剩余 CommitLog/dispatcher/flush-manager owner 仍须收口；执行清单仍为完成 7 项、剩余 24 项。

M11-12bc69 随 Issue #8551 完成 CommitLog 子服务所有权收窄：LocalFileMessageStore 直接拥有
`CommitLogDispatcherDefault` registry，每次独占注册后通过 ArcSwap 发布不可变 dispatcher generation；CommitLog 与
Reput worker 只持 `CommitLogDispatchHandle`，异步 dispatch 在 await 前取得稳定 snapshot，不持同步锁跨 await。
CommitLog 同时直接拥有 `DefaultFlushManager`，start/shutdown/graceful shutdown 均沿既有独占 lifecycle 驱动；零外部
调用的 `CommitLog::new` 收窄为 crate 内 composition 入口。reviewed baseline 从 133/476 降至 132/466；production
从 60/149 降至 59/139，test 保持 59/287，compatibility 保持 14/40；Store production 从 56/141 降至
55/131。相对 bc68 净删除 1 identity/10 occurrences；3 个保留 Reput `ArcMut<CommitLog>` occurrence 因相邻
dispatcher 参数/字段变化，经临时 ADR-013 一对一指纹审核，approval 未提交。R10 从 6/29 降至 6/23，R13 从
3/11 降至 2/7；两项仍未完成，执行清单仍为完成 7 项、剩余 24 项。

M11-12bc70 随 Issue #8553 删除 CommitLog 的完整 LocalStore 回指：`CommitLogStoreContext` 只包含标准 Arc/atomics、
不可变 delay metadata 和通过 ArcSwapOption 发布的 HA snapshot；put/confirm/stats 路径只读取该窄 context。四条 recovery
入口不再接收 `ArcMut<LocalFileMessageStore>`，completion 直接使用 CommitLog 已拥有的 ConsumeQueueStore；Local delay table
同步改为标准 Arc。CommitLog tests 还删除了经 production glob 传播的 ArcMut import 和两个返回类型 occurrence。reviewed
baseline 从 132/466 降至 128/453；production 从 59/139 降至 57/129，test 从 59/287 降至 57/284，compatibility
保持 14/40；Store production 从 55/131 降至 53/121，Store test 从 47/200 降至 45/197。相对 bc69 净删除
4 identities/13 occurrences；唯一保留 test constructor 因改用全限定路径，经临时 ADR-013 一对一 occurrence relocation
审核，approval 未提交。R10 从 6/23 降至 6/20，R13 从 2/7 降至 0/0 并完成；执行清单现为完成 8 项、剩余 23 项。

M11-12bc71 随 Issue #8555 删除 Local 长期读者的完整 CommitLog owner：`MappedFileQueueReadHandle` 只观察 ArcSwap
mapped-file generation、mapped-file size 与共享 runtime progress；`CommitLogReadHandle` 只组合该读句柄、不可变配置、
`CommitLogStoreContext` 和原子 confirm state。Reput、后台索引重建、scheduled self-check、compaction payload 与 tiered
dispatch resolver 均改持只读 capability；controller/duplication/SyncFlush/AsyncFlush confirm policy 由 facade 与 handle 共用
同一 resolver。reviewed baseline 从 128/453 降至 128/447；production 从 57/129 降至 57/123，test 保持 57/284，
compatibility 保持 14/40；Store production 从 53/121 降至 53/115。相对 bc70 净删除 6 个 production occurrences、
无新增 identity。唯一保留的 Local composition-root constructor occurrence 经临时 ADR-013 一对一审核，approval 未提交。
R10 从 6/20 降至 6/14，仍未完成；执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

M11-12bc72 随 Issue #8557 删除 Local commit-log maintenance 的共享引用可变逃逸：MappedFileQueue create、
truncate 与 reset 改为 `&self`，以共享 runtime maintenance lock 串行化并通过 ArcSwap generation 发布；并发创建相同
offset 复用已发布文件。CommitLog facade 同步改为共享引用，LocalFileMessageStore 的 reset、truncate 与 last-file
create 删除 3 处 `mut_from_ref`。reviewed baseline 从 128/447 降至 127/444；production 从 57/123 降至
56/120，test 保持 57/284，compatibility 保持 14/40；Store production 从 53/115 降至 52/112。相对 bc71
净删除 1 个 production identity/3 occurrences，无 relocation、新增 identity 或临时 approval。R10 从 6/14 降至
5/11，仍未完成；执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

M11-12bc73 随 Issue #8561 删除 Local 根中的完整 CommitLog 共享 owner：LocalFileMessageStore 直接持有组合构造的
`CommitLog`，不再保存 `ArcMut<CommitLog>`；后台 reader、cleanup、flush、dispatcher 与 Store context 仍只持既有窄
capability，recovery/start/shutdown 和 mutable facade 继续使用 Local 根独占借用。reviewed baseline 从 127/444 降至
127/442；production 从 56/120 降至 56/118，test 保持 57/284，compatibility 保持 14/40；Store production
从 52/112 降至 52/110。相对 bc72 净删除 2 个 production occurrences，无 relocation、新增 identity 或临时 approval。
R10 从 5/11 降至 5/9，仍未完成；执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

M11-12g1 随 Issue #8563 修复 bc73 验证发现的当前 main telemetry 基线回归：6 个 log-filter metric 补入 Rust
catalog，generator 同步 canonical registry 与 violation fixture，精确 inventory 更新为 125 metrics、136 signals、
68 attributes；telemetry semantic guard、8/8 Python 合同与 observability all-feature 测试通过。该门禁修复不涉及
ArcMut owner/occurrence，因此 reviewed baseline 保持 127/442，R10 保持 5/9，执行清单仍为完成 8 项、剩余 23 项，
正式进度仍为 75/82；下一切片为 M11-12bc74。

M11-12bc74 随 Issue #8565 删除 Local 根对完整自身句柄的直接保留：`set_message_store_arc` 只执行一次性子依赖
wiring，不再把 `ArcMut<LocalFileMessageStore>` 保存到字段；ConsumeQueue/Timer 保持既有兼容下发，Default/
AutoSwitch HA 通过 pending child 在 `init` 原边界完成包装、初始化与发布。reviewed baseline 从 127/442 降至
127/440；production 从 56/118 降至 56/116，test 保持 57/284，compatibility 保持 14/40；Store production
从 52/110 降至 52/108。相对 bc73 净删除 2 个 production occurrences，3 个保留 occurrence 经临时 ADR-013
一对一 relocation 审核，approval 仅位于忽略的 `target/`，无新增 identity 或提交态 approval。R10 从 5/9 降至
5/7，仍未完成；执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

M11-12bc75 随 Issue #8567 删除 Default HA client 的共享可变 runtime root：`DefaultHAClient.inner` 从
`ArcMut<Inner>` 改为标准 `Arc<Inner>`，共享部分只保留原子、异步锁、Notify、flow monitor 与既有 LocalStore
兼容句柄；从未安装的 read/write stream 和重复 dispatch/read/backup/report buffer 已删除，实际 buffer 继续归每个
reader/writer connection task 独占。reviewed baseline 从 127/440 降至 126/437；production 从 56/116 降至
55/113，test 保持 57/284，compatibility 保持 14/40；Store production 从 52/108 降至 51/105。相对 bc74
净删除 1 个 production identity/3 occurrences，唯一保留的 `Inner.default_message_store` occurrence 经临时
ADR-013 一对一 relocation 审核，approval 仅位于忽略的 `target/`，无新增 identity 或提交态 approval。R15 从
9/29 降至 8/26，仍未完成；执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

M11-12bc76 随 Issue #8569 删除 ConsumeQueueStore 的共享可变组合根：cloneable adapter 从
`ConsumeQueueStoreRoot<ArcMut<Inner>>` 改为标准 `Arc<Inner>`，唯一晚绑定的 LocalFileMessageStore 兼容句柄由
短 `RwLock<Option<_>>` 保护；lookup、type check 与 queue constructor 都在 clone 句柄后释放 guard，不扩大文件或异步
锁域。reviewed baseline 从 126/437 降至 126/435；production 从 55/113 降至 55/111，test 保持 57/284，
compatibility 保持 14/40；Store production 从 51/105 降至 51/103。相对 bc75 净删除 2 个 production
occurrences，5 个保留 occurrence 经临时 ADR-013 一对一 relocation 审核，approval 仅位于忽略的 `target/`，
无新增 identity 或提交态 approval。R12 从 17/36 降至 17/34，仍未完成；执行清单仍为完成 8 项、剩余 23 项，
正式进度仍为 75/82。

M11-12bc77 随 Issue #8571 删除 ConsumeQueue trait-object 的 unchecked shared-mutable alias：
`ArcConsumeQueue` 改为标准 `Arc` + 每队列 `RwLock<Box<dyn ConsumeQueueTrait>>` handle，Store、Timer 与 Broker
调用方显式选择 read/write guard；迭代器仅在 read guard 生命周期内使用，Broker schedule delivery 逐条读取并在 await
前释放 guard，不引入 Store 全局锁。reviewed baseline 从 126/435 降至 103/391；production 从 55/111 降至
34/71，test 从 57/284 降至 55/280，compatibility 保持 14/40；Store production 从 51/103 降至
30/63。相对 bc76 净删除 23 identities/44 occurrences，无 relocation、新增 identity 或临时 approval。R09 从
8/13 降至 4/7、R10 从 5/7 降至 3/4、R11 从 5/7 降至 3/4、R12 从 17/34 降至 4/6，均仍未完成；
执行清单仍为完成 8 项、剩余 23 项，正式进度仍为 75/82。

M11-12bc78 随 Issue #8573 删除 ConsumeQueue 的完整 LocalStore compatibility carrier：ConsumeQueueStore 只保存
标准 `Arc` 组成的配置/topic table/running flags/checkpoint 与 `CommitLogReadHandle` 窄上下文，single queue
只持该上下文和标准 `Weak` find-or-create queue lookup；CommitLog timestamp lookup、LMQ 首次建队与已有 offset
路径保持原语义。reviewed baseline 从 103/391 降至 99/385；production 从 34/71 降至 30/65，test 保持
55/280，compatibility 保持 14/40；Store production 从 30/63 降至 26/57。相对 bc77 净删除 4 个
production identities/6 occurrences，无 relocation、新增 identity 或临时 approval。R12 从 4/6 降至 0/0 并完成；
执行清单现为完成 9 项、剩余 22 项，正式进度仍为 75/82。

M11-12bc79 随 Issue #8575 删除 Timer delivery 的共享可变逃逸：CommitLog 单条/批量追加在既有 topic-queue lock
与 put-message lock 下接受共享引用，LocalFileMessageStore 的 `MessageStore` 兼容入口委托给共享实现，rolled/due
Timer 消息不再调用 `mut_from_ref`。reviewed baseline 从 99/385 降至 98/383；production 从 30/65 降至
29/63，test 保持 55/280，compatibility 保持 14/40；Store production 从 26/57 降至 25/55。相对 bc78
净删除 1 个 production identity/2 occurrences，无 relocation、新增 identity 或临时 approval。R14 从 3/7
降至 2/5，仍未完成；执行清单保持完成 9 项、剩余 22 项，正式进度仍为 75/82。

M11-12bc80 随 Issue #8577 将 LocalFile lifecycle probe 的 shared-owner 构造收回所有者边界：`bench_support`
不再导入或直接构造 `ArcMut<LocalFileMessageStore>`，crate-private `local_file_shared_owner` factory 负责兼容
shared owner 创建与一次性 self-wiring，并通过 opaque return 隐藏具体指针。reviewed baseline 从 98/383
降至 97/382；production 从 29/63 降至 28/62，test 保持 55/280，compatibility 保持 14/40；Store
production 从 25/55 降至 24/54。旧 probe constructor identity 一对一迁入 owner 模块，旧 import identity
真实删除；临时 ADR-013 approval 仅位于忽略的 `target/`。R09 从 4/7 降至 2/5，R10 从 3/4 调整为
4/5；执行清单保持完成 9 项、剩余 22 项，正式进度仍为 75/82。

M11-12bc81 随 Issue #8579 删除 LocalFile lifecycle probe 的临时 shared-owner factory：probe 直接独占
`LocalFileMessageStore`，owned wiring 仅在 MessageStore/Broker duplication mode 一致开启且 Timer 关闭时成功，
否则以 `InvalidState` fail closed；既有 init/start、四项 scheduled task 统计与 clean shutdown 语义保持。
reviewed baseline 从 97/382 降至 96/381；production 从 28/62 降至 27/61，test 保持 55/280，
compatibility 保持 14/40；Store production 从 24/54 降至 23/53。相对 bc80 净删除 1 个 production
identity/1 occurrence，无 relocation、新增 identity 或临时 approval。R10 从 4/5 降至 3/4；执行清单保持
完成 9 项、剩余 22 项，正式进度仍为 75/82。

M11-12bc82 随 Issue #8581 将 DefaultHAClient 的完整 LocalStore carrier 收窄为 replica-store capability：
`MappedFileQueueAppendHandle` 共享既有 generation、allocator 与 maintenance state，`CommitLogReplicaHandle`
继续使用同一个 put-message lock、runtime state 与 checkpoint，`HAReplicaStoreHandle` 只补充 Store config 和
shutdown 边界；ReaderTask 的 raw append、physical offset 与 confirm clamp/publication 不再获得 queue/index/dispatcher
等无关能力。Default/AutoSwitch client 测试 helper 同步改为 owned Store。reviewed baseline 从 96/381 降至
89/371；production 从 27/61 降至 25/56，test 从 55/280 降至 50/275，compatibility 保持 14/40；
Store production 从 23/53 降至 21/48。相对 bc81 净删除 7 identities/10 occurrences（production 2/5、
test 5/5），无 relocation、新增 identity 或临时 approval。R15 从 8/26 降至 6/21；执行清单保持完成
9 项、剩余 22 项，正式进度仍为 75/82。

M11-12bc83 随 Issue #8583 将 Default HA connection registry 改为唯一 owner：accept path 构造并启动
`GeneralHAConnection` 后把 owned value 移入注册表，不再构造或克隆 `ArcMut<GeneralHAConnection>`；移除和关闭
均先从表中取出/drain，再在锁外完成状态通知或 shutdown。`connection_state` 通过已有原子/RwLock 组成的
`HAConnectionRuntimeHandle` 跨锁读取，不持连接表 guard 跨 `.await`。reviewed baseline 从 89/371 降至
88/366；production 从 25/56 降至 24/51，test 保持 50/275，compatibility 保持 14/40；Store production
从 21/48 降至 20/43。净删除 1 个 production identity/5 occurrences，无 relocation、新增 identity 或临时
approval。R15 从 6/21 降至 5/16；执行清单保持完成 9 项、剩余 22 项，正式进度仍为 75/82。

M11-12bc84 随 Issue #8585 将 DefaultHAService 的完整 LocalFile Store carrier 收窄为既有 replica-store
capability：Default/AutoSwitch service 与 connection 的 segment transfer、confirm offset、master flush、alive replica、
state-machine version 和 controller epoch 通过同一个 `HAReplicaStoreHandle` 读写原共享状态，不获得 queue、index、
dispatcher 或完整 Store lifecycle。CommitLog/MappedFileQueue 的原 transfer 实现下沉到 read/replica handle，facade
继续委托同一算法。reviewed baseline 从 88/366 降至 88/364；production 从 24/51 降至 24/49，test 保持
50/275，compatibility 保持 14/40；Store production 从 20/43 降至 20/41。净删除 2 个 production
occurrences；22 个保留 test occurrence 经临时 ADR-013 同 item 一对一 relocation 审核，approval 仅位于忽略的
`target/`，无新增 identity 或提交态 approval。R15 从 5/16 降至 5/14；执行清单保持完成 9 项、剩余 22 项，
正式进度仍为 75/82。

M11-12bc85 随 Issue #8587 将 Default HA connection service roots 收窄为窄运行时上下文：accept socket、
`DefaultHAConnection`、reader 与 writer task 不再持有或导入完整 `ArcMut<DefaultHAService>`，仅获得 connection
count、replica store、replication progress、transfer metrics 与必要 callback；连接注册表、group-transfer service 和
state-notification service 通过标准 `Weak` 访问，连接退出后不会反向保活 service graph。reviewed baseline 从
88/364 降至 84/351；production 从 24/49 降至 21/37，test 从 50/275 降至 49/274，compatibility 保持
14/40；Store production 从 20/41 降至 17/29。净删除 4 identities/13 occurrences（production 3/12、
test 1/1），无 relocation、新增 identity 或临时 approval。R15 从 5/14 降至 2/2，剩余仅 Default HA 组合根
初始化 carrier；执行清单保持完成 9 项、剩余 22 项，正式进度仍为 75/82。

M11-12bc86 随 Issue #8589 删除 Default/AutoSwitch HA 初始化中的重复共享可变边界：`DefaultHAService::init`
改为只接受独占 `&mut Self`，`AutoSwitchHAService` 直接拥有 `DefaultHAService` delegate，不再创建内部
`ArcMut<DefaultHAService>`，client construction、role transition、replication callbacks、runtime info 与 shutdown
继续委托同一 owned delegate。reviewed baseline 从 84/351 降至 81/347；production 从 21/37 降至 18/33，
test 保持 49/274，compatibility 保持 14/40；Store production 从 17/29 降至 14/25。净删除 3 个 production
identities/4 occurrences；2 个保留 import occurrence 经临时 ADR-013 同 item 指纹审核，approval 仅位于忽略的
`target/`，无新增 identity 或提交态 approval。R15 从 2/2 降至 1/1、R16 从 5/9
降至 3/6；剩余 import-only debt 与 General HA 外层组合根一起收口。执行清单保持完成 9 项、剩余 22 项，
正式进度仍为 75/82。

M11-12bc95 随 Issue #8607 收窄 LocalFile 单测根所有权：普通 fixture 直接返回独占
`LocalFileMessageStore`，通用 helper 关闭 Timer 并复用 owned-root capability wiring；仅三个确实验证 Timer
组合关系的兼容测试继续通过具名 legacy helper 显式持有 `ArcMut`，没有用 opaque return 隐藏完整 Store owner。
reviewed baseline 从 56/225 降至 55/210：production 保持 13/24，test 从 29/161 降至 28/146，
compatibility 保持 14/40；Store test/bench caller 从 21/145 降至 20/130，净删除 1 个 test identity/
15 occurrences。两个保留 occurrence 经忽略的临时 ADR-013 同 item relocation 审核，无新增 identity 或提交态
approval。R17 上界降至 28/146；执行清单保持完成 11 项、剩余 20 项，正式进度仍为 75/82。

M11-12bc96 随 Issue #8609 批量收窄跨 crate LocalFile test roots：既有 owned-root wiring 提升为带
`# Errors` 契约的 additive public composition API，Timer enabled 仍 fail-closed；CommitLog recovery、HA
semantics 与 Broker expression-filter fixture 直接拥有 concrete root，BrokerRuntime role tests 改为复用
production message-store 初始化入口。reviewed baseline 从 55/210 降至 44/196：production 保持 13/24，
test 从 28/146 降至 17/132，compatibility 保持 14/40；Store test/bench caller 从 20/130 降至
14/123，Broker test caller 从 6/8 降至 1/1，净删除 11 test identities/14 occurrences，无 relocation、
新增 identity 或临时 approval。R17 上界降至 17/132；执行清单仍为完成 11 项、剩余 20 项，正式进度仍为
75/82。

M11-12bc97 随 Issue #8611 提取 Timer Store capability：活跃路径只持队列查询、CommitLog 读取和内部消息
写入句柄，LocalFile owned wiring 在 Timer enabled 时不再要求完整 Store root；共享 hook registry、生命周期状态、
flush 唤醒与 reput 通知均通过窄句柄发布。Timer 单测、恢复集成测试和 LocalFile Timer fixture 直接拥有 concrete
Store，删除全部 test `mut_from_ref` 与 wrapper；恢复回归还修复 ConsumeQueue truncate 持读锁再申请同队列写锁的
重入死锁。reviewed baseline 从 44/196 降至 35/90：production 保持 13/24、test 从 17/132 降至 8/26、
compatibility 保持 14/40；Store test/bench caller 从 14/123 降至 5/17，净删除 9 test identities/106
occurrences；两个保留公开兼容 occurrence 仅使用忽略的临时 ADR-013 同 item relocation approval，无新增 identity
或提交态 approval。R14 活跃路径完成，公开 legacy 2/5 转由 R18 兼容窗口；
R17 上界降至 8/26，执行清单现为完成 12 项、剩余 19 项，正式进度仍为 75/82。

M11-12bc98 随 Issue #8615 完成 Store owned-root caller 迁移：Broker LocalFile 初始化、RocksDB delegate、
CommitLog benchmark 与中英文 README 示例统一调用 `wire_owned_root_dependencies`，仓库内部不再调用
`set_message_store_arc`；benchmark 直接独占 concrete LocalFile root，并用 `join_all` 并发轮询共享引用的
CommitLog 写入 future，保留锁竞争测量而不再共享完整 Store。Broker 同时删除无调用方的
`BrokerRuntimeInner::set_message_store` wrapper。reviewed baseline 从 35/90 降至 31/73：production 从
13/24 降至 13/23、test 从 8/26 降至 4/10、compatibility 保持 14/40；净删除 4 identities/17
occurrences，无 relocation、新增 identity 或临时 approval。R10 活跃 caller 已完成，公开 legacy 2/2 转由
R18；R17 上界降至 4/10，执行清单现为完成 13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc99 随 Issue #8617 引入 non-exhaustive `OwnedMessageStore` application composition root：Local
variant 直接拥有 `LocalFileMessageStore`，Rocks variant 直接拥有 `RocksDBMessageStore`；Broker production
从 `ArcMut<Generic<ArcMut<Backend>>>` 收窄为单一外层 `ArcMut<Owned<Backend>>`，删除 Local/Rocks 两个
内层完整 Store wrapper。既有公开 `GenericMessageStore` variant、constructor 与 legacy forwarding 原样保留，
R11 的公开 Rocks Local-root getter 也未改变。reviewed baseline 从 31/73 降至 31/71：production 从
13/23 降至 13/21、test 保持 4/10、compatibility 保持 14/40；Broker production 从 4/7 降至 4/5，
Store production 保持 9/16。净删除 2 production occurrences、无新增 identity；两个继续存在的外层
constructor 仅使用忽略的临时 ADR-013 同 item 一对一 relocation approval。R01 尚未完成，执行清单仍为
完成 13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc100 随 Issue #8621 收窄 Broker fast-failure 的 Store 依赖：page-cache busy checker 不再捕获完整
`ArcMut<OwnedMessageStore>`，而是只捕获已有 `PutMessagePreflight` 原子只读能力与
`os_page_cache_busy_timeout_mills` 标量，Local/Rocks 两条初始化路径保持同一语义。初始化完成且尚未构造 Admin
runtime 时，完整 Store 强 owner 从 3 个降为 2 个，仅由 `BrokerRuntimeInner` 与晚绑定 `EscapeBridge` 保留。
reviewed baseline 保持 31 identities/71 occurrences（production 13/21、test 4/10、compatibility 14/40、
Broker production 4/5、Store production 9/16），无 identity relocation、新增债务或 baseline 变更。R01 尚未
完成，下一切片拆分 Admin owner；执行清单仍为完成 13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc101 随 Issue #8623 收窄 Broker Admin 的 Store 所有权：`BrokerAdminRuntime` 不再保存
`LegacyEscapeStoreOwner`，改为克隆已有 `Weak<EscapeBridge>`；读操作借用 `EscapeBridge` 已绑定的 owner，必须可变的
兼容操作仅在单次请求期间取得 write lease。构造和 clone Admin runtime、以及把 Admin runtime clone 送入异步注册动作，
均不再增加完整 Store root 强引用；Local/Rocks Admin 查询、写入、read-ahead 配置和 Timer 显式 handle 行为保持不变。
reviewed baseline 保持 31 identities/71 occurrences（production 13/21、test 4/10、compatibility 14/40、
Broker production 4/5、Store production 9/16），无 identity relocation、新增债务或 baseline 变更。R01 尚未
完成，下一切片继续拆分 EscapeBridge/lifecycle owner；执行清单仍为完成 13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc102 随 Issue #8625 唯一化 Broker Store lifecycle owner：`BrokerRuntimeInner` 直接持有唯一长期
`Arc<LegacyEscapeStoreOwner<_>>`，`EscapeBridgeStoreCapability` 改为标准 `Weak` provider，只在单次读取、写入或
生命周期操作期间升级/租借；Local/Rocks Store 构造集中到私有 owner factory，删除 owner 的通用 `Clone`/`DerefMut`
传播入口。Broker init/load/start/shutdown、hook/listener wiring 均使用短期 write lease；释放 Broker owner 后 provider
fail closed，Admin clone 不增加外层或 legacy pointer 强计数。嵌入式 `rocketmq-proxy-local` 同步采用仓库已有的
256 codegen 查询深度预算，覆盖完整构建中的 Broker worker future 布局计算而不改变运行时行为。reviewed baseline 从
31/71 降至 28/67：production
从 13/21 降至 11/18、test 从 4/10 降至 3/9、compatibility 保持 14/40；Broker production 从 4/5 降至
2/2、Store production 保持 9/16。净删除 3 identities/4 occurrences；保留 constructor identity 与私有字段
occurrence 仅使用忽略的临时 ADR-013 一对一 relocation approval，无提交态 approval。R01 仍剩私有 legacy owner
2/2，R17 降至 3/9；执行清单保持完成 13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc103 随 Issue #8627 删除 Admin 完整可变 Store 逃逸：`BrokerAdminRuntime::message_store_mut`、
`EscapeBridge::lease_message_store_mut` 与 capability 通用 write-lease 已删除，production Admin/processor
只暴露 append、commit-log read-mode 与 topic-delete 三个具名操作；测试 Store 启动回到 Broker composition root，
size-limited read 改为普通读借用，不再克隆 mutable carrier。owner 释放后读取和三个具名写操作均 fail closed，
源码契约禁止恢复完整 write lease。reviewed baseline 保持 28 identities/67 occurrences（production 11/18、
test 3/9、compatibility 14/40、Broker production 2/2、Store production 9/16），无 identity relocation、新增
债务或 baseline 变更。R01 尚未完成，下一切片优先提取普通消息共享 append capability；执行清单保持完成
13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc104 随 Issue #8629 提取 Broker 普通消息共享 append 边界：`OwnedMessageStore` 为 Local/Rocks
组合根提供隐藏共享单条/批量写入，EscapeBridge 通过持有同一生命周期 owner 标准 `Weak` 的私有强类型端口执行
普通单条、批量和 Admin append；请求路径不再克隆完整可变 Store carrier，也不引入 dyn async 热路径分配。
Local/Rocks 写入回执、普通批次 ConsumeQueue 单元、HA/flush、hook/LMQ、reput 与派生队列语义保持，owner
释放后端口 fail closed。reviewed baseline 保持 28 identities/67 occurrences（production 11/18、test 3/9、
compatibility 14/40、Broker production 2/2、Store production 9/16），无 identity relocation、新增债务或
baseline 变更。R01 尚未完成，下一切片继续提取 role/admin control 与 read/lifecycle capability；执行清单保持完成
13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc105 随 Issue #8631 收窄 Broker Store 控制操作：controller role-change 在 HA await 前取得标准
`GeneralHAService` 窄 clone，完整 mutable Store wrapper 不再跨异步边界；role sync、CommitLog read-mode 与
topic-delete 只在 owner 具名同步方法内部使用瞬时 compatibility wrapper，请求层不能取得通用 mutable lease。
原 write lease 重命名并限制为 composition-root lifecycle lease，仅服务 init、hook wiring、start 与 shutdown。
主从 epoch/address、动态 role/config、Local/Rocks topic deletion 与 owner 释放后的 fail-closed 行为保持。
reviewed baseline 保持 28 identities/67 occurrences（production 11/18、test 3/9、compatibility 14/40、
Broker production 2/2、Store production 9/16），无 identity relocation、新增债务或 baseline 变更。R01
尚未完成，下一切片提取 read/lifecycle capability 并删除私有 legacy owner；执行清单保持完成 13 项、剩余
18 项，正式进度仍为 75/82。

M11-12bc106 随 Issue #8633 将 Broker Store 生命周期修改收口为独占访问：删除
`LegacyEscapeStoreLifecycleLease`，初始化、hook wiring、processor listener、load、start 与 shutdown 在修改
Store 前统一解绑 EscapeBridge 的弱 provider，并由外层 `Arc::get_mut` 证明组合根没有其他强/弱 owner；需要继续
服务请求的路径完成修改后重新绑定。shutdown 不再重新绑定，并在共享绝对 deadline 内等待已接纳的请求读租约
退出，超时则由统一 shutdown deadline 报告失败，不越过 deadline 强行取得可变引用。Local/Rocks 初始化、共享追加、
hook、processor dispatch 与 owner 释放后的 fail-closed 语义保持。reviewed baseline 保持 28 identities/67
occurrences（production 11/18、test 3/9、compatibility 14/40、Broker production 2/2、Store production
9/16），无 identity relocation、新增债务或 baseline 变更。R01 尚未完成，下一切片提取剩余只读 capability 并
删除私有 legacy owner；执行清单保持完成 13 项、剩余 18 项，正式进度仍为 75/82。

M11-12bc107 随 Issue #8635 删除 Broker 最后的私有 legacy Store owner：`BrokerRuntimeInner` 直接持有
`Arc<OwnedMessageStore>`，EscapeBridge 与 Admin runtime 只保存 Weak provider，`EscapeStoreReadLease` 与共享
append port 只在单次请求期间升级标准 Arc。Local/Rocks/Owned 的 read-mode、topic-delete、role-sync 改为共享
借用，动态 broker role 与 read-ahead 通过 Store 原子运行态发布给 CommitLog、Timer、offset correction 与 Reput；
未引入同步大锁、unsafe 可变别名或替代 wrapper。生命周期仍先解绑 provider，再通过 `Arc::get_mut` 取得独占 Store；
Broker 配置代际继续同步发布动态 role/read-ahead。reviewed baseline 从 28/67 降至 26/65（production
11/18→9/16、test 保持 3/9、compatibility 保持 14/40），Broker production 从 2/2 降至 0/0，Store production
保持 9/16；净删除 2 个 production identities/occurrences，无 relocation、新债务或临时 approval。R01 完成，
执行清单现为完成 14 项、剩余 17 项，正式进度仍为 75/82。

M11-12bc108 随 Issue #8637 将 RocksDB Store 组合根改为独占 owner：
`RocksDBMessageStore.local_file_store` 从 `ArcMut<LocalFileMessageStore>` 改为
`Box<LocalFileMessageStore>`，并删除仓库内零调用的 `local_file_store_arc()` 完整 mutable root clone
accessor；LocalFile borrowed access、trait dispatch、recovery 与 lifecycle 行为保持。reviewed baseline 从
26/65 降至 23/61（production 9/16→6/12、test 保持 3/9、compatibility 保持 14/40），Store production
同步从 9/16 降至 6/12；净删除 3 个 production identities/4 occurrences，无 relocation、新债务或临时
approval。R11 完成，执行清单现为完成 15 项、剩余 16 项，正式进度仍为 75/82。

M11-12bc109 随 Issue #8639 收口真实 test/bench caller：RocksDB semantics helper 不再以
`ArcMut` 包装独占 RocksDB root，而是直接构造 `OwnedMessageStore`；16 个 Broker Store-capability 测试模块也
从 `GenericMessageStore` type-only 参数迁到生产使用的 owned root。Generic Store trait-contract 与 ArcMut
facade 自测继续作为明确 compatibility fixture 保留。reviewed baseline 从 23/61 降至 22/60（production
保持 6/12、test 3/9→2/8、compatibility 保持 14/40）；净删除 1 个 test identity/occurrence，无 relocation、
新债务或临时 approval。R17 完成，执行清单现为完成 16 项、剩余 15 项，正式进度仍为 75/82。

M11-12bc110 随 Issue #8641 收口 stable nightly surface：`RequestProcessorV2::Fut` 公开 GAT API 保持，
Remoting core built-in processor 以 concrete `Ready` future 保持零堆分配，integration/example 的异步
实现显式使用 boxed future；crate、test 与 example 因此删除三处 `impl_trait_in_assoc_type`。Controller
当前 `Arc<Self>` receiver 无需 `arbitrary_self_types`，对应 gate 同步删除。新增 baseline/target
stable-surface guard 与 7 项正负/源码合同，仓库 nightly feature 属性从 8 个降至 4 个。ArcMut baseline
保持 22/60，R22 仍未完成；剩余 Runtime 2 个 feature 与 R18 兼容面 2 个 feature，target 模式按预期
fail closed。执行清单保持完成 16 项、剩余 15 项，正式进度仍为 75/82。

M11-12bc111 随 Issue #8643 收口 Runtime scheduler nightly surface：保留四个公开 `_async` 方法名，将
lending `AsyncFnMut` 约束改为 `FnMut(CancellationToken) -> Send + 'static Future`，由一层内部 Tokio
mutex 在 future 完成前保持独占调用语义，再复用既有稳定 scheduler；删除约 150 行重复调度实现。
Runtime、Client、Broker 的 22 个捕获型调用点统一在返回 `async move` future 前克隆 owned capability，
不新增完整 Store/runtime root 捕获。Runtime 两个 feature gate 删除后，仓库 nightly feature 属性从 4 个
降至 2 个，stable Runtime all-target、4 项调度语义、runtime ownership audit 与 Proxy Local 递归回归通过。
ArcMut baseline 保持 22/60，R22 仍未完成；剩余两处均为 R18 `sync_unsafe_cell`，完整 stable matrix 继续等待
同一冻结候选快照。执行清单保持完成 16 项、剩余 15 项，正式进度仍为 75/82。

M11-12bc112 随 Issue #8645 收口最后的 nightly surface：`ArcMut`、`WeakArcMut` 与
`SyncUnsafeCellWrapper` 改用 workspace 已有的 stable `parking_lot::RwLock` backing，保留公开名称以及
constructor、downgrade/upgrade、unwrap、Serde、Deref、equality/hash 等 legacy 行为；`get_inner()` 返回
stable `Arc<RwLock<T>>`，新代码可使用读写 guard。legacy reference escape 仍逐处标注外部同步责任，因此本切片
不把 R18 公开 facade 删除伪报完成。零调用的 `syncunsafecell_mut` comparison benchmark 与 manifest entry
一并删除，避免重新引入 test/bench ArcMut caller。两处 `sync_unsafe_cell` gate 清零后，stable-surface
baseline/target guard 均报告 0 feature，stable default 与 workspace all-target/all-feature 矩阵在同一候选
快照通过；R22 完成。reviewed ArcMut baseline 从 22/60 降至 20/58（production 保持 6/12、test
2/8→1/7、compatibility 14/40→13/39），无新增 governed debt；执行清单现为完成 17 项、剩余 14 项，
正式进度仍为 75/82。

M11-12bc113 随 Issue #8647 完成 R23 技术审计：版本化 Python runner 在
`target/architecture-refactor/` 下生成隔离 Cargo probe，直接依赖当前 `rocketmq-rust`。Miri guarded
`get_inner().read/write` 路径通过；两个 ArcMut clone 仅通过 safe `DerefMut` 建立可变别名时，Miri 以
Stacked Borrows/Unique retag 证据报告预期 UB。runner 只有在 guarded exit=0、alias exit 非零且包含目标 UB
诊断时通过，4 项分类器单测覆盖意外成功、无关工具失败和目标诊断。Loom 2/2 验证
`Arc<RwLock<T>>` writer serialization 与 worker 强 owner 完整释放；Loom 0.7.2 不支持 `Weak`，未虚报该
交错覆盖。审核结论是不存在可长期保留的 sound ArcMut wrapper，R18 仍须在 next-major/HUMAN 窗口删除公开
facade。ArcMut baseline 保持 20/58，nightly feature 保持 0；R23 完成，执行清单现为完成 18 项、剩余
13 项，正式进度仍为 75/82。

M11-12bc114 随 Issue #8649 完成 R24 工程交付：versioned policy 固定六小时 soak、60 秒采样、1% 缺样上限、
五项 semantic-registry metric 目标和两项动态 fault 目标；Grafana dashboard、Prometheus alerts、英文
runbook、rollback assertions 与 SHA-256 evidence index 由 guard 精确对齐。真实 runner 先消费同一候选 image
map 的 M11-11 dynamic fault evidence，再从 Prometheus API 采样；production commit、五服务 digest、目标、
回滚和 artifact 任一不一致均 fail closed。9 项正向/故意违规测试与 Validate mode 通过；本机无
Docker/Kind/K3d/Kubectl/Helm/真实镜像/Secret/Prometheus endpoint，因此未虚报动态 PASS，R20/R21/R25 保持
开放。R24 完成，执行清单现为完成 19 项、剩余 12 项，正式进度仍为 75/82。

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
