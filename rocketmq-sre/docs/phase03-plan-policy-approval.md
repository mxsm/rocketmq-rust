# Phase 3 Plan、Policy、Approval 与 Audit

Control Plane 是计划、策略和人工审批的唯一入口。模型只能提交结构化候选参数；服务端会重新读取诊断、Evidence 和 ActionDescriptor，并重新计算 `evidence_hash`、每一步的 `precondition_hash` 与最终 `plan_hash`。

## 确定性边界

- 只有状态为 `confirmed`、`execution_eligible=true` 且绑定真实 `primary_model_invocation_id` 的诊断可以创建 ActionPlan。
- R1、R2 都必须经过人工审批。R2 在异构 Critic 完成前保持 `needs_critic`。
- R3 只能返回 `ManualRunbookDraft`；未知 action 直接拒绝。
- 策略只读取服务端事实，模型输出不能覆盖 deny。
- 审批人必须拥有 `approver` role、目标 cluster scope，且不能审批自己创建的计划。
- ApprovalGrant 由 Control Plane 使用 `ROCKETMQ_SRE_GRANT_SIGNING_KEY` 签发，只面向 `rocketmq-sre-executor` audience。
- 执行提交时再次读取 Evidence、锁、quarantine 与计划状态。plan hash、precondition hash、审批有效期或 live state 任一变化都会 fail closed。
- quarantine 解除需要 approver、目标 cluster scope、原因和至少一个仍有效的人工验证 Evidence。

## 配置

确定性策略保存在 `config/policy/supervised-execution.v1.yaml`。生产模式必须配置：

```text
ROCKETMQ_SRE_GRANT_SIGNING_KEY=<至少 32 字节的独立随机值>
```

该值只进入 Control Plane 进程，不写入数据库、日志、Audit 或 API 响应。开发模式未设置该变量时，会使用现有内部开发 token 作为本地 fixture key。

## API

```text
POST /v1/plans
GET  /v1/plans/{id}
POST /v1/plans/{id}/approve
POST /v1/plans/{id}/reject
POST /v1/executions
GET  /v1/executions/{id}
GET  /v1/events/stream
GET  /v1/audit/{correlation_id}
GET  /v1/resource-quarantines
POST /v1/resource-quarantines/{id}/clear
```

所有写操作使用或生成同一个 correlation ID，并追加到 PostgreSQL `audit_events`。SSE 只向同 tenant 且具有对应 cluster scope 的订阅者推送脱敏事件。
