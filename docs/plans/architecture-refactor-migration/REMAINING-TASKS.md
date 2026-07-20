# 架构重构剩余任务盘点

> 盘点日期：2026-07-20
> 代码基线：Issue #8423 / M11-12bc9 完成后
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

Issue #8423 后 reviewed ArcMut baseline 为 394 identities / 1,014 occurrences：production 230/529、test
150/445、compatibility 14/40。production 全部分布在 Broker 与 Store。

| owner | 剩余 identity / occurrence | 完成条件 |
|---|---:|---|
| Broker | 120 / 241 | transaction bridge、Producer/ColdData admin leaf 与 Schedule hook 已退出完整 runtime owner；继续让显式 Store 兼容边界、BrokerRuntime aggregate carrier、其余 admin/processor/service 不再传播不安全共享可变 owner |
| Store | 110 / 288 | BrokerStats observer、ConsumeQueueExt owner 与 HA notification/connection registry capability 已收窄；其余 MessageStore、CommitLog/Flush、queue、Rocks/Timer 与 HA 生命周期改为独占 owner、标准 Arc/Weak 或显式 actor/锁边界 |
| compatibility | 14 / 40 | 删除 `rocketmq/src/arc_mut.rs` production/public re-export 与兼容入口 |

建议按以下最小可审查批次继续推进；它们是 PR-M11-12 的内部切片，不增加 82 个顶层工作包总数：

1. Broker aggregate：收窄 `BrokerRuntimeInner`、processor variant 和启动 carrier（Broker 当前为 120/241）；Schedule hook 强保活环已拆除，继续清理只读取少量能力的 admin/processor leaf。
2. Broker leaf：完成其余 admin/processor/revive/slave/offset leaf owner；transaction bridge 已由 M11-12bc4 收窄，Producer/ColdData admin handler 已由 M11-12bc5 改持 live registry/standard Arc capability，Schedule hook 已由 M11-12bc6 改持三项显式能力。
3. Store WAL：收口 Local/Rocks MessageStore、CommitLog 与 Flush manager，并替换 transaction 的直接 Store 兼容 owner。
4. Store queue：ConsumeQueueExt 已改用显式锁 owner；继续收口其余 ConsumeQueue、queue store、index/mapped-file carrier。
5. Store timer/HA：BrokerStats observer、HA notification service 与 connection registry 查询已退出完整 Store/connection owner；继续收口 Timer、Default/General/AutoSwitch HA service、client 与 connection actor。
6. compatibility：迁移剩余测试/兼容调用方，删除公开 ArcMut facade 和不再需要的 nightly surface。
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
