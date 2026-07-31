# RocketMQ AI SRE 自身运维 Runbook

本文对应 `rocketmq.sre.self-slo.v1` 和
`rocketmq.sre.self-degradation-policy.v1`。AI SRE 不是 RocketMQ 数据面的依赖：
AI SRE 故障必须停止新的变更副作用，但不能停止 Producer、Consumer、Broker、
NameServer、Controller 或 Proxy。

## 通用处置顺序

1. 确认 RocketMQ 数据面探针是否仍为成功；数据面异常时按 RocketMQ 独立
   Runbook 处理，不能把 AI SRE 恢复动作当成数据面恢复动作。
2. 检查 Control Plane、Connector、Executor、Execution Agent 和 PostgreSQL
   `/readyz` 或 Kubernetes Ready 状态。
3. 查询 workflow/Evidence/notification backlog、lease/fence rejection、
   Unknown effect、quarantine 和 probe cleanup 信号。
4. PostgreSQL、Executor、Execution Agent、lease/fence 或审计异常时立即冻结新
   mutation。已开始的 verification/compensation 按 journal 继续；无法证明结果
   时进入 Unknown 并人工接管。
5. 仅允许自动重启一个无状态 Connector 或 Provider adapter，或触发只读诊断。
   不允许自动重启数据库、Executor、Execution Agent，不允许清除 quarantine、
   推进 fence、丢弃 Unknown effect 或绕过审计。
6. 恢复后核对 error budget、积压、Unknown/quarantine、审计完整性和数据面探针，
   再由人工解除 freeze。Paused 自治只能恢复到 Shadow 或 Supervised。

## ai-sre-control-plane-unavailable

- 影响：不能创建新的调查、计划或审批；新 mutation 被拒绝。
- 检查：Pod Ready、`/readyz`、数据库状态、workflow queue、最近部署事件。
- 恢复：回滚最近的 Control Plane 发布或恢复其依赖；不要修改 RocketMQ 资源。
- 完成条件：`/readyz` 成功，workflow queue 不再增长，审计可写，数据面探针成功。

## ai-sre-connector-evidence-stale

- 影响：Evidence 查询降级；超出 freshness 的证据不能用于计划或自治。
- 检查：Connector `/readyz`、MCP 握手、token 轮换、cluster allowlist 和最老心跳。
- 自动恢复：只允许重启一个无状态 Connector，并等待 Ready replacement。
- 降级：Provider 可用时仍不得解释过期 Evidence；切换为 rules-only 或返回
  `source_unavailable`。
- 完成条件：心跳小于 120 秒，所需 Evidence queryable 且 freshness 恢复。

## ai-sre-executor-unavailable

- 影响：所有新 mutation 停止；只读调查和诊断保持可用。
- 检查：Executor `/readyz`、PostgreSQL、journal、resource lock、lease 和 pending
  effect。
- 恢复：人工恢复 Executor；从 journal 对账，不自动重启、不重放未确认副作用。
- 完成条件：所有 pending execution 可判定为成功、失败或 Unknown；无重复 effect。

## ai-sre-agent-or-fence-failure

- 影响：新 mutation 必须冻结；RocketMQ 数据面不受影响。
- 检查：Agent `/readyz` 和 capabilities、fence rejection、highest epoch、pending
  nonce、Unknown effect、active quarantine。
- 恢复：人工恢复 Agent 或 lease authority；先对账旧 epoch effect，再激活新 epoch。
- 禁止：自动重启 Agent、强制推进 fence、删除 effect、清除 quarantine。
- 完成条件：epoch 单调、无未对账 effect；quarantine 仅经审计人工解除。

## ai-sre-probe-failure-or-cleanup

- 影响：生产验证不可用；不得用缺失值或 0 伪装成功。
- 检查：专用 Topic/Group、消息数量/大小/时限、Job TTL 和专用凭据。
- 恢复：清理固定前缀且已终止的 probe Job；禁止删除业务 Topic/Group。
- 完成条件：有界探针精确完成 send/receive/ack，且没有过期 Job。

## ai-sre-provider-degraded

- 影响：AI 增强诊断或 Critic 不可用；确定性 rules-only 诊断保持可用。
- 检查：实际 provider/model identity、错误率、延迟、fallback、token 和成本预算。
- 自动恢复：可重启一个无状态 Provider adapter；随后按实际 identity 建立新 cohort。
- 降级：依次 fallback，全部失败时 rules-only；缺少异构 Critic 时拒绝 R1 自治和
  R2 执行。
- 完成条件：健康 smoke 通过、预算未超限、fallback 身份已记录。

## ai-sre-postgresql-unavailable

- 影响：Control Plane 非就绪；所有新 workflow 和 mutation 停止。
- 检查：PostgreSQL Pod/磁盘/连接数/延迟和 migration；不要在告警或日志中输出
  database URL。
- 恢复：由数据库 owner 人工恢复或故障转移，AI SRE 不自动操作数据库。
- 完成条件：migration 一致、审计和 journal 可读写、pending effect 对账完成。

## ai-sre-outbox-backlog

- 影响：通知、自治事件或外部集成延迟；待发送记录必须保留。
- 检查：notification/autonomy/integration outbox 的最老 pending age、重试次数和
  下游状态。
- 恢复：恢复下游并重试相同幂等键；不得丢弃事件或绕过需要通知的审批。
- 完成条件：最老 pending 小于 300 秒，无重复通知，失败原因已脱敏。

## 月度自身可靠性报告

每月从 `autonomy_operational_reports` 生成 `period_kind=month` 报告，至少包含：

- 八项服务 SLO 达成率和 error budget 消耗。
- workflow/Evidence/notification 积压和恢复时间。
- Provider 错误、fallback、token/成本和预算告警。
- mutation deny/freeze、lease/fence rejection、Unknown effect 和 quarantine。
- Probe 成功率、清理情况，以及 AI SRE 故障期间 RocketMQ 数据面探针结果。

报告不得包含 token、secret、TLS/ACL material、消息正文、客户端地址或完整配置。
