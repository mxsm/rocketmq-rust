# M05：`rocketmq-transport` 边界提取

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | M05 已完成；等待 Gate 审阅 |
| 预计周期 | 3–4 周 |
| 工作包 | WP10 `transport-boundary-spike`；集成 WP03/WP04/WP05 |
| 前置条件 | PendingRequestGuard、TaskGroup lease、绝对 deadline 稳定；protocol contract 与 wire corpus冻结 |
| 可并行项 | codec/net 与 client/server 模块在接口冻结后可并行，remoting facade 切换串行 |
| 完成后解锁 | M07、M08 |

## 目标

- 创建拥有 TCP/TLS/codec/session/admission/request correlation/client/server 的 `rocketmq-transport`。
- 复用 M02 生命周期原语，建立 count+byte 双预算和连接关闭的 complete-all 语义。
- transport 单向依赖 protocol/security-api/runtime/error/optional observability，不依赖业务 service 或 auth provider。
- remoting 退化为 protocol+transport 兼容 facade，并保持现有默认 TLS 与 feature 名称。

## 非目标

- 不改变 wire schema、request code 或 Broker domain processor 语义。
- 不同时重写 connection_v2 和现网络栈；V2 必须经单 request benchmark 决定保留或删除。
- 不把 auth provider、Broker、Store、完整 Client 高层实现下沉到 transport。

## 入口条件

- [x] `[ARCH]` 冻结 transport/protocol/security/runtime 的依赖方向和 request lifecycle 状态机。
- [x] `[TEST]` 准备 TLS on/off、pending race、close-all、overload 和 collector outage 场景。
- [x] `[DEV]` 确认 remoting 目标文件无用户修改重叠，记录当前 feature/default/cargo tree。
- [x] `[HUMAN]` 批准 overload 时的协议错误/连接关闭策略和 per-session/global 预算层级。

## 交付物

| 类型 | 交付物 |
|---|---|
| Crate | `rocketmq-transport`，`default = [tls]`，可选 `observability` |
| Network | codec、buffer、channel、session、pipeline、client pool、server connection task、TLS |
| Lifecycle | PendingRequestGuard、TaskGroup child lease、absolute deadline、close-all |
| Admission | max frame/header/body/inflight/queued bytes、handshake/connection/processor budgets |
| Security | transport → SecurityRequestView adapter；provider 由 composition 注入 |
| Compatibility | remoting 精确 re-export/alias；feature/default 保持 |
| Evidence | TLS/feature matrix、race/overload/soak、V2 决策报告 |

## PR 级开发步骤

### PR-M05-01：创建 crate 与单 request lifecycle spike

- [x] `[ARCH]` 选择与 M04 相同或相邻的真实 request code，固定 client→writer→socket→reader→processor→response 流程。
- [x] `[DEV]` 创建 crate/模块骨架并加入 workspace；先迁一个 request lifecycle，保持实现拓扑和行为。
- [x] `[DEV]` 使用 protocol canonical 类型、M02 PendingRequestGuard 和 ServiceContext，不复制 schema。
- [x] `[TEST]` 对旧 remoting 与新 transport 做成功、超时、send failure、close 的差分。
- [x] `[REV]` 检查 request 只注册一次、task 有 owner、无 detached spawn/runtime。
- [x] 回滚点：remoting 继续保留旧实现，spike adapter 可整体移除。

### PR-M05-02：迁移 codec、buffer 与 net primitives

- [x] `[DEV]` 迁 codec、adaptive encode buffer、channel、session、pipeline、local harness 和 transport config。
- [x] `[DEV]` initial read buffer 按需增长；max frame/header/body 在分配前校验；buffer pool 按 bytes 限制。
- [x] `[DEV]` common/remoting 的 ServerConfig/TlsConfig/TlsMode 旧路径精确 re-export，Serde/default 不变。
- [x] `[TEST]` 覆盖 fragmented/oversized/malformed frame、buffer growth/release、local harness 和 config round-trip。
- [x] `[REV]` 检查无全局 1 MiB eager buffer、无无界 channel、无消息 body 日志。
- [x] 回滚点：逐模块 re-export 回旧实现；config envelope 不删除。

### PR-M05-03：迁移 client、RPC runtime 与 pending table

- [x] `[DEV]` 迁 async/blocking client、connection pool、有状态 RPC metadata/hook/address resolution。
- [x] `[DEV]` opaque reservation 在 enqueue 前完成；冲突/回绕时 drain 并更换连接，不复用活跃 opaque。
- [x] `[DEV]` response/timeout/send-failure/close/Drop 走 complete-once，释放 count/byte permit。
- [x] `[TEST]` 运行 10k timeout、response-timeout、writer-timeout、close-late-response 和 reconnect race。
- [x] `[REV]` 检查 late response 不串请求、map/permit 不泄漏、blocking facade 只在 approved boundary。
- [x] 回滚点：保留 remoting old-client adapter，单独撤销 connection pool 切换。

### PR-M05-04：迁移 server、processor adapter 与 shutdown

- [x] `[DEV]` 迁 request processor/runtime adapter、server connection task 和 shutdown；Broker processor 仍留业务 owner。
- [x] `[DEV]` accept/handshake/session/processor/writer task 都注册为 ServiceContext/TaskGroup child。
- [x] `[DEV]` 连接关闭主动 shutdown socket，并以 typed cause 完成全部 pending/queued response。
- [x] `[TEST]` 覆盖 hung processor、half-close、TLS handshake timeout、server drain 和单一绝对 deadline。
- [x] `[REV]` 检查没有 sync lock guard 跨 await，OS thread/BlockingExecutor 都进入资源报告。
- [x] 回滚点：server composition 可重新选择旧 remoting implementation；public facade 不变。

### PR-M05-05：有界 admission 与 security adapter

- [x] `[ARCH]` 为 global/per-IP/per-tenant/session 定义 count+byte budget、满载策略和 metric owner。
- [x] `[DEV]` 实现 connection/handshake/inflight/queued-bytes/processor concurrency gate。
- [x] `[DEV]` 将 protocol command 借用投影为 SecurityRequestView，注入 RequestPolicy/OutboundSigner；不依赖 rocketmq-auth。
- [x] `[TEST]` overload 下验证显式拒绝/关闭、RSS 有界、控制面仍可用、collector outage 不阻塞数据面。
- [x] `[REV]` 检查每个 channel 声明 capacity、bytes、full policy、owner、metrics；字段低基数且脱敏。
- [x] `[HUMAN]` 批准默认预算值只基于 profile 证据发布；无证据时保留现默认并要求显式配置。
- [x] 回滚点：保留 budget interface，可回滚默认值或 admission 接线；不得恢复无界 channel。

### PR-M05-06：remoting facade、feature 与 V2 决策

- [x] `[DEV]` remoting 对 protocol/transport 精确 re-export，已迁实现从 facade 删除。
- [x] `[DEV]` 保持 remoting `default = [tls]`，`simd/tls/observability` 弱转发到 owner。
- [x] `[TEST]` 运行 no-default/default/TLS/observability/组合 feature 与 canonical/legacy compile fixture。
- [x] `[TEST]` 对一个真实 request 比较 V1/V2 兼容、吞吐、p99、RSS、维护复杂度。
- [x] `[ARCH]` 若 V2 未同时优于兼容、性能、维护性，移除平行实现；若保留，限定 internal experimental feature。
- [x] `[REV]` 证明 transport closure 不含 Broker/Store/Client 高层/auth provider/common/legacy。
- [x] `[HUMAN]` 批准 V2 keep/delete 结论和 M05 Gate。

## 公共兼容面

- remoting 的 public module/root/prelude/深路径和 feature 名保留一个 major。
- TLS 默认和现有 client `use_tls` 语义不变；新预算默认值不得在同一机械迁移 PR 中改变。
- TransportError 到 wire response 的映射保持 response code/header 兼容，错误分类留 `rocketmq-error`。
- connection_v2 不升级为稳定公共栈，除非 Human Architect 批准独立版本化提案。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-remoting
cargo test -p rocketmq-broker
cargo test -p rocketmq-client-rust --lib
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-error-hygiene.ps1
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
git diff --check
```

### 本里程碑新增后执行

```powershell
cargo check -p rocketmq-transport --no-default-features
cargo check -p rocketmq-transport
cargo test -p rocketmq-transport --no-default-features
cargo test -p rocketmq-transport
cargo test -p rocketmq-transport --features observability
cargo tree -p rocketmq-transport -e normal
python scripts/architecture_dependency_guard.py --mode baseline
python scripts/arc_mut_guard.py
```

## 回滚触发器

- pending request 双完成、错配、泄漏或连接关闭未完成全部 waiter。
- TLS/default/Serde/wire 行为发生未批准变化。
- overload 仍可导致无界 RSS/channel，或 collector 故障阻塞数据面。
- transport closure 引入业务 service、完整 Client、common/legacy 或 auth provider。
- shutdown 超过绝对 deadline，或出现 detached task/thread。

回滚按 client、server、codec、admission 切片执行，remoting facade 始终保可切换 adapter。正确性/有界性修复不因性能未达假设而回滚；优化实现而不恢复泄漏。

## Exit Checklist

- [x] `[TEST]` pending race、10k timeout、close-all、TLS/overload/collector outage 全绿。
- [x] `[REV]` transport normal closure满足禁边，所有 background work 有 owner。
- [x] `[DEV]` remoting 仅 facade/re-export，feature/default 与基线一致。
- [x] `[TEST]` 每个 channel 的 count+byte 上界可通过测试和 metrics 观察。
- [x] `[ARCH]` V2 keep/delete 有证据和书面结论。
- [x] `[DEV]` workspace/package policy 与新增 transport 一致。
- [x] `[HUMAN]` 预算、错误映射和 M05 Gate 已签署。

## M05 completion evidence

- Canonical ownership and compatibility paths are recorded in
  `05-transport-compatibility-ledger.md`; legacy config, codec, connection, pending-table, smart-buffer, and
  error-helper paths are exact re-exports, while TLS keeps a narrow lifecycle compatibility adapter.
- The lifecycle suite covers successful request/response, send failure, hung processors, half-close, late
  response isolation, owner rotation, close-all, count+byte release, and 10,000 simultaneous expirations.
- Frame and buffer tests cover fragmented frames, allocation-before-limit rejection, bounded pool growth and
  release, and legacy configuration Serde/default behavior. Existing codec tests retain malformed-frame cases.
- Admission tests exercise global/per-IP/per-tenant/per-session count and byte limits, bounded scope cardinality,
  control reserve, explicit reject/close policy, and a full or dropped one-slot collector.
- TLS is validated both disabled and enabled, including optional/required/mutual modes, certificate reload,
  handshake failure/timeout cancellation, and graceful shutdown under the owning `TaskGroup`.
- `connection_v2` was deleted: it had no production consumer, no independent feature boundary, and no usable
  end-to-end request composition to benchmark. It therefore failed the compatibility and maintainability gates
  before performance could justify retaining a second stack.
- The M05 ArcMut inventory promotion reduces governed identities from 1,266 to 1,233 and occurrences from 3,430
  to 3,378. The final promotion consumes two strict one-to-one same-item fingerprint relocations approved by
  ADR-013; the canonical transport crate contains no ArcMut use.

## 交接物

- 向 M07 交付 protocol+transport canonical import、RouteLookup 所需 client 和 ServiceContext 范式。
- 向 M08 交付 proxy ingress 可依赖的 transport/security 边界。
- 向 M09 交付 remoting facade ledger、feature matrix 和 V2 决策。
- 向 M11 交付 admission/queued-bytes/connection SLI 和 shutdown hooks。
