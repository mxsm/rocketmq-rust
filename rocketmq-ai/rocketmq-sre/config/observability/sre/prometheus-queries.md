# RocketMQ AI SRE Phase 1 Prometheus 查询

本文件对应 `rocketmq.sre.observability-metrics.v1`。所有查询只使用低基数标签，不按租户、集群、Incident、Evidence、模型名或 Connector ID 聚合。

## Incident

最近 5 分钟启动的诊断：

```promql
sum(increase(rocketmq_sre_incidents_total{outcome="started"}[5m]))
```

当前运行中的诊断：

```promql
rocketmq_sre_incidents_active
```

rules-only 占比：

```promql
sum(rate(rocketmq_sre_incidents_total{outcome="rules_only"}[15m]))
/
clamp_min(sum(rate(rocketmq_sre_incidents_total{outcome=~"completed|rules_only"}[15m])), 1)
```

## Evidence

按来源查看 P95 查询耗时：

```promql
histogram_quantile(
  0.95,
  sum by (le, source) (rate(rocketmq_sre_evidence_query_duration_seconds_bucket[5m]))
)
```

按来源和错误类型查看错误速率：

```promql
sum by (source, result) (
  rate(rocketmq_sre_evidence_query_errors_total[5m])
)
```

## Diagnostic Pack

按 Pack 查看 P95 评估耗时：

```promql
histogram_quantile(
  0.95,
  sum by (le, pack) (rate(rocketmq_sre_diagnostic_evaluation_duration_seconds_bucket[5m]))
)
```

## Model Gateway

模型请求与错误速率：

```promql
sum by (provider) (rate(rocketmq_sre_model_requests_total[5m]))
```

```promql
sum by (provider, result) (rate(rocketmq_sre_model_errors_total[5m]))
```

输入/输出 token：

```promql
sum by (provider, direction) (increase(rocketmq_sre_model_tokens_total[1h]))
```

过去一小时估算成本（美元）：

```promql
sum by (provider) (increase(rocketmq_sre_model_cost_microusd_total[1h])) / 1000000
```

## Read-only Tool

按工具类别和结果查看调用速率：

```promql
sum by (tool_class, result) (rate(rocketmq_sre_tool_calls_total[5m]))
```

## 告警建议

- Evidence 某来源 10 分钟内错误率持续高于 20%：检查 Connector、MCP 和对应后端。
- Provider 全部不可用：标记 AI 增强能力降级，但不要阻断 rules-only 诊断。
- 数据库 `unavailable`：`/readyz` 返回非就绪。
- Connector 全部不可用：服务进程仍可就绪，但 `evidenceCollectionAvailable=false`。

查询和 Dashboard 不应增加任何高基数标签。需要定位单次请求时，请使用相同的 `x-correlation-id` 在 Tempo/Loki 中检索。
