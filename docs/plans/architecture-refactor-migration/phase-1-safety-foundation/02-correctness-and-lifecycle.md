# M02：P0 正确性、请求生命周期与关闭时序

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 1：安全性与基础治理 |
| 状态 | 已完成（2026-07-11） |
| 预计周期 | 3–4 周 |
| 工作包 | WP02 `flush-result`、WP03 `task-child-lease`、WP04 `pending-request-guard`、WP05 `shutdown-deadline`；WP01 首个迁移切片 |
| 前置条件 | M01 guard/baseline 可用；行为修复与源码提取分 PR |
| 可并行项 | flush、TaskGroup、pending request 三条 PR 链可并行，但共享 runtime/remoting/store 文件必须串行持有 writer lease |
| 完成后解锁 | M05、M06、M07 |

## 目标

- 新增 canonical `try_flush() -> Result<FlushProgress, StoreError>`，修复 flush 失败仍推进 durable watermark 或丢失错误的问题。
- 让动态 TaskGroup child 完成后自动摘除，shutdown 使用单一绝对 deadline。
- 让 pending request 在 response、timeout、send failure、close 和 Drop 路径恰好完成一次。
- 用明确所有权原语替换 runtime/request table 的首批高风险 ArcMut。

## 非目标

- 不在本里程碑提取 transport/store-local crate。
- 不顺便重写网络栈、CommitLog 或所有服务的 Runtime API。
- 不以任意 sleep 编写并发测试，不降低现有 runtime/error baseline。

## 入口条件

- [x] `[ARCH]` 冻结 SyncFlush 确认语义、TaskGroup child 生命周期和 shutdown phase 顺序。
- [x] `[TEST]` 为四条工作流确定 deterministic test、virtual time、故障注入和 kill/restart 路线。
- [x] `[DEV]` 确认目标文件没有与用户未提交修改重叠。
- [x] `[HUMAN]` 已批准兼容方案 1：新增 fallible `try_flush`；R0 保留旧 `flush() -> i64` adapter 及下述失败契约。

## 交付物

| 类型 | 交付物 |
|---|---|
| Store | canonical `try_flush() -> Result<FlushProgress, StoreError>`；R0 legacy `MessageStore::flush() -> i64` adapter；只在成功后推进 watermark |
| Runtime | dynamic child lease、Weak/summary registry、active/history/outcome metrics |
| Network | `PendingRequestGuard`、一次注册、`complete_once`、close-all typed cause |
| Shutdown | `ShutdownDeadline`、阶段预算、未完成项 `ShutdownReport` |
| Soundness | runtime/request table 首批 ArcMut 使用量下降，无新安全可变逃逸 |
| Tests | flush failure/crash、100k child churn、10k timeout、race、hung processor/deadline |

## PR 级开发步骤

### PR-M02-01：先锁定 Flush 失败回归

- [x] `[TEST]` 增加可控 I/O failure fixture，证明失败前旧实现会错误推进或丢失错误。
- [x] `[ARCH]` 定义 `FlushProgress` 的 appended/durable watermark 与 error 分类，不把派生引擎进度混入 append receipt。
- [x] `[DEV]` 新增 canonical `try_flush() -> Result<FlushProgress, StoreError>`；flush/group commit 只有在 fsync 成功后才原子推进 durable watermark。
- [x] `[DEV]` 所有 SyncFlush、group-commit waiter 和消息 ack 路径只能调用 `try_flush`，禁止经 legacy `flush() -> i64` 判断成功。
- [x] `[DEV]` R0 保留 `MessageStore::flush() -> i64` adapter；调用 `try_flush` 失败时返回**最后一次成功的 durable watermark**，将 Store 标记为不可写，并把完整 typed error 写入 `StoreHealth` 与低基数 telemetry。
- [x] `[DEV]` 同批 SyncFlush waiter 共享同一 `try_flush` 结果；失败以同一 `StoreError` 完成全部 waiter，不能把 legacy 返回值解释为本批成功。
- [x] `[REV]` 检查 error source 未被字符串化、失败路径不 panic、ack 路径没有提前完成，legacy adapter 也没有推进或伪造 watermark。
- [x] `[TEST]` 执行进程 crash/restart，验证成功确认消息 100% 可见、失败消息未被误报成功。
- [x] `[TEST]` 增加 legacy adapter 契约测试：失败返回上次 durable watermark、`writable=false`、health/telemetry 保留 typed cause，且任何 ack spy 都未调用 legacy `flush`。
- [x] 回滚点：R0 旧签名始终保留；若 `try_flush` 接线失败，停止 SyncFlush/ack 发布并修复 canonical 路径，不得回滚到由 `i64` 推断刷盘成功。

### PR-M02-02：TaskGroup 动态 child lease

- [x] `[TEST]` 先增加 100k create/complete churn 测试，断言 active child 回到初始值且历史强引用不累积。
- [x] `[ARCH]` 冻结 lease/token 的 Drop、显式 complete、panic、cancel 和 parent shutdown 状态机。
- [x] `[DEV]` 将静态 child 与动态 child 分开；父 registry 只保留 Weak/summary，最后 task 退出自动摘除。
- [x] `[DEV]` 增加 active/history/pruned/panic/cancel/deadline metrics，字段低基数且不记录敏感数据。
- [x] `[REV]` 检查 lease 不造成循环 Arc，Drop 路径幂等，锁 guard 不跨 await。
- [x] `[TEST]` 覆盖正常、panic、cancel、parent-first、child-first 和注册/关闭竞态。
- [x] 回滚点：保留旧 `TaskGroup` public facade；只回滚内部 registry，不要求调用方改回新 API。

### PR-M02-03：PendingRequestGuard 与 complete-once

- [x] `[TEST]` 先覆盖 response-timeout、timeout-writer、close-late-response 三组竞态和 10k 永不响应请求。
- [x] `[ARCH]` 冻结 permit → opaque reservation → enqueue → complete 的顺序，以及 opaque 冲突/回绕策略。
- [x] `[DEV]` enqueue 前原子注册；writer 不再重复插入；guard 持有 oneshot、deadline、permit 和 created_at。
- [x] `[DEV]` response、timeout、send failure、connection close、Drop 统一调用 `complete_once`，移除 map 并释放 permit。
- [x] `[DEV]` close 主动 shutdown socket，并以 typed cause 完成所有 pending。
- [x] `[REV]` 检查迟到 response 不能完成新请求，map 与 permit 不会双重释放或泄漏。
- [x] `[TEST]` 使用虚拟时间/显式 barrier，不使用固定端口或任意等待。
- [x] 回滚点：该 PR 只修改现有 remoting 内部实现和兼容 API；失败时恢复旧 table 但保留新竞态测试作为阻塞证据。

### PR-M02-04：统一绝对 ShutdownDeadline

- [x] `[TEST]` 构造挂起 processor、blocking job、socket 和 telemetry，测量总退出时间而非每层超时。
- [x] `[ARCH]` 固定阶段：reject new work → drain sessions → flush/replicate → stop background → telemetry flush → report。
- [x] `[DEV]` 从进程入口计算单一绝对 deadline，RuntimeOwner/ServiceContext/TaskGroup/BlockingExecutor 只消费剩余预算。
- [x] `[DEV]` 把未完成 task/thread/resource 记录到 typed `ShutdownReport`，正常目标 `leaked_tasks=0`。
- [x] `[DEV]` 首先对 Broker 注入 context，并保持现有同步 facade 仅使用 audit-approved 顶层边界。
- [x] `[REV]` 检查没有嵌套重新获得完整 30 秒、detached spawn/thread 或嵌套 runtime。
- [x] `[TEST]` 证明挂起组件时总退出仍在 deadline 内，并准确报告未完成项。
- [x] 回滚点：保留兼容构造入口，把新 deadline 适配器撤出 composition；不得删除报告测试。

### PR-M02-05：首批 ArcMut 与 controller downcast 清理

- [x] `[ARCH]` 按 Arc/lock/atomic/actor/owner 分类 runtime 和 pending table 状态。
- [x] `[DEV]` 替换首批安全共享可变逃逸，写明 atomic ordering 或锁保护不变量。
- [x] `[DEV]` controller 类型不匹配改为 typed error；不再静默 downcast 失败。
- [x] `[TEST]` 运行 Miri/Loom 可覆盖的局部模型和现有回归测试。
- [x] `[REV]` 对每个 unsafe block 检查紧邻 `SAFETY` contract，安全 wrapper 必须自行建立唯一性。
- [x] `[TEST]` 证明 ArcMut baseline 严格下降、新增为零。
- [x] 回滚点：逐切片回滚，不能恢复被证明错误的 silent downcast 或 watermark 行为；此类冲突升级 Human Architect。

## 公共兼容面

- `TaskGroup` 和现有 remoting/store public facade 保留；canonical 刷盘入口固定为 `try_flush() -> Result<FlushProgress, StoreError>`。
- R0 保留 `MessageStore::flush() -> i64`：成功返回本次 durable watermark；失败返回最后成功 watermark、标记 Store 不可写，并通过 typed health/telemetry 暴露错误。该 adapter 只服务旧调用方，SyncFlush/ack 禁止调用。
- request code、opaque wire 字段、Serde、存储格式和当前 feature 默认值不变。
- shutdown 行为变得有界，但公开配置字段与现有默认值的改变必须独立版本化，不在机械修复 PR 中完成。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-runtime
cargo test -p rocketmq-remoting
cargo test -p rocketmq-store
cargo test -p rocketmq-controller
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-error-hygiene.ps1
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
git diff --check
```

迭代期先运行命名测试；以上 package/全 workspace 命令在最终快照执行。

### 本里程碑新增后执行

```powershell
cargo test -p rocketmq-store flush_failure_does_not_advance_watermark
cargo test -p rocketmq-store legacy_flush_failure_returns_last_durable_watermark
cargo test -p rocketmq-store sync_flush_ack_uses_try_flush_only
cargo test -p rocketmq-runtime dynamic_child_registry_returns_to_baseline
cargo test -p rocketmq-remoting pending_request_complete_once
cargo test -p rocketmq-remoting pending_requests_are_released_after_timeout
cargo test -p rocketmq-runtime shutdown_uses_one_absolute_deadline
python scripts/arc_mut_guard.py
```

命名以实施时实际测试符号为准，但证据索引必须映射到上述五种行为，不能以不存在的命令记为通过。

## 回滚触发器

- SyncFlush/ack 仍调用 legacy `flush`，或 `try_flush` 失败仍可能返回成功或推进 watermark。
- legacy adapter 失败时没有返回最后成功 watermark、没有将 Store 标记不可写，或 typed health/telemetry 丢失 cause。
- child/pending 数在 churn 后不回到基线，或竞态测试出现双完成/错配。
- shutdown 总耗时随组件层数线性叠加，或引入 detached task/thread。
- 新 API 要求破坏 wire/storage/public signature，且没有获批兼容 adapter。
- ArcMut guard 增长或 runtime/error audit 新增债务。

回滚以 PR 为单位；正确性 bug 修复不可仅因性能回归直接撤销，应先保留正确性并优化实现。公共兼容冲突必须暂停并交由 `[HUMAN]` 决策。

## Exit Checklist

- [x] `[TEST]` `try_flush` failure 不推进 watermark，同批 waiter 收到同一 typed error。
- [x] `[TEST]` legacy adapter 失败契约和“所有 SyncFlush/ack 只调用 `try_flush`”契约通过。
- [x] `[TEST]` 已成功确认消息 crash/restart 后 100% 可见。
- [x] `[TEST]` 100k child churn 后 active 回基线，历史强引用不增长。
- [x] `[TEST]` 10k timeout 后 pending map 为 0。
- [x] `[REV]` 所有完成路径均 complete-once，无锁 guard 跨 await。
- [x] `[TEST]` hung component 场景在单一绝对 deadline 内结束并生成报告。
- [x] `[DEV]` ArcMut baseline 严格下降且没有新增安全 escape。
- [x] `[ARCH]` 行为修复与后续 crate 提取保持独立提交。
- [x] `[HUMAN]` canonical/legacy flush 兼容方案已批准；里程碑结束时只需复核实现证据与剩余风险。

## 交接物

- 向 M05 交付稳定的 PendingRequestGuard、TaskGroup lease 和 shutdown contract。
- 向 M06/M10 交付 `try_flush`/legacy adapter/result/watermark 语义与故障 corpus。
- 向 M07/M11 交付 ServiceContext 注入范式和绝对 deadline 报告。
