# Phase 00 协议兼容性

## 固定版本

| Surface | Phase 00 版本 |
| --- | --- |
| MCP protocol | `2025-11-25` |
| MCP business schema | `rocketmq-mcp.v2` |
| Canonical Evidence | `rocketmq-sre.evidence.v1` |
| Capability catalog | `rocketmq-sre.capability-catalog.v1` |
| Required signals | `rocketmq.sre.required-signals.v1` |

## 接受与拒绝规则

- 同一 major 中新增可选字段：接受并忽略未知可选字段。
- 未知 major、缺失 required feature：拒绝。
- Tool schema digest、Tool surface 或 Resource surface 不一致：握手失败，
  集群进入 `read_only_degraded`。
- MCP 声明 mutation capability：拒绝连接并进入 `rejected`。
- Evidence hash 不一致：拒绝该 Evidence，不交给模型。
- Incident 的 `resolved`/`escalated` 终态不得返回运行态。
- Offboarding 使用 tombstone + identity revoke，保留能力快照和审计事件。

稳定错误 envelope 至少包含 `code`、脱敏 `message`、`retryable` 与
`correlation_id`。Phase 00 必须支持：

- `unsupported_schema_major`
- `missing_required_feature`
- `schema_digest_mismatch`
- `capability_mismatch`
- `unauthorized_scope`
- `tenant_mismatch`
- `cluster_not_allowed`
- `output_too_large`
- `source_unavailable`

## 模型协议适配矩阵

Phase 00 只提供 ProviderDescriptor 与能力 fixture，不进行外部网络调用。

| Provider | 协议族 | Streaming | Tools | Structured output | Embeddings |
| --- | --- | ---: | ---: | ---: | ---: |
| OpenAI-compatible | OpenAI | 是 | 是 | 是 | 是 |
| Anthropic | Anthropic Messages | 是 | 是 | 是 | 否 |
| Gemini | Google Gemini | 是 | 是 | 是 | 是 |
| AWS Bedrock | Bedrock Converse | 是 | 是 | 是 | 取决于模型 |
| DeepSeek | OpenAI-compatible | 是 | 是 | 是 | 否 |
| 智谱 GLM | OpenAI-compatible / GLM | 是 | 是 | 是 | 是 |
| Kimi / Moonshot | OpenAI-compatible | 是 | 是 | 是 | 否 |
| 本地模型 | OpenAI-compatible | 取决于运行时 | 取决于模型 | 取决于模型 | 取决于模型 |

真实 Provider 认证、速率限制、成本路由、网络调用与模型回退属于后续阶段。
