# Phase 03 异构模型 Critic

P3-04 为 R2 计划增加了不可绕过的异构模型复核。Critic 只负责输出固定结构的复核结论，不能修改 ActionDescriptor、PolicyDecision、ActionPlan、参数或执行状态。

## 异构判定

Control Plane 从计划固化的 `incident_id + diagnosis_revision + primary_model_invocation_id` 读取真实 `ModelInvocationRecord`。模型家族经过去空白、统一大小写和分隔符归一化后比较：

- 相同 `model_family` 的不同 endpoint、region、revision 或 profile alias 仍是同一模型家族。
- Critic 候选必须支持 Chat、Text、JSON Schema、当前数据分级且处于可路由健康状态。
- fallback 只能在与 primary 不同的模型家族中进行。
- 没有异构候选时写入 `Unavailable` review，计划保持 `NeedsCritic`。

## 固定复核面

Critic schema 只能报告：

- Evidence 引用和反证；
- 参数是否在允许范围；
- 遗漏的前置条件；
- 影响范围是否符合 descriptor；
- 回滚是否可用；
- `accept`、`needs_revision` 或 `reject` 结论。

Control Plane 会再次校验 Evidence ID、参数 schema、forbidden fields、descriptor 版本、影响范围、回滚配置、Policy 和计划 hash。模型输出没有覆盖这些本地判定的能力。结论与结构化检查冲突时记录为 `Conflict`，不能进入审批。

## 持久化与状态机

调用接口：

```text
POST /v1/plans/{id}/critic
```

请求只包含当前 `plan_hash`。成功或降级结果均以 append-only `critic_reviews` 保存，并绑定：

- plan hash 和 diagnosis revision；
- primary invocation；
- Critic 实际 invocation、provider、profile、规范化 model family、revision 和 endpoint instance；
- 实际 fallback 链；
- prompt/schema version；
- review payload hash、结论和固定 assessment。

Critic 模型调用以 `purpose=critic` 写入 `model_invocations`，并设置
`parent_invocation_id=primary_model_invocation_id`。只有
`status=valid + conclusion=accept + 实际异构 invocation` 会在同一数据库事务中将计划从 `NeedsCritic` 推进到 `ReadyForApproval`。

R1 计划不依赖 Critic，但 `GET /v1/plans/{id}` 会明确返回
`critic_state=unreviewed_not_required`。R2 的 unavailable、invalid、
conflict、needs-revision 和 rejected 状态都不会解锁审批。

## 验证

```powershell
$env:ROCKETMQ_SRE_TEST_DATABASE_URL = `
  'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre'
cargo +1.95.0 test --locked -p rocketmq-sre-control-plane `
  supervised_execution::critic_tests::postgres_ -- --ignored --nocapture
```

真实 PostgreSQL 测试覆盖 DeepSeek 失败后 fallback 到 Kimi 的实际身份绑定，以及同一模型家族更换 alias/endpoint 后仍保持不可审批。
