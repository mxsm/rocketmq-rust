# AI SRE 协议兼容性

## 固定版本

| Surface | 基线版本 |
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
`correlation_id`。兼容层至少支持：

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

Model Gateway 已实现 Canonical Model IR、Provider profile、同步/异步 HTTP
transport、流式响应、有界输出、稳定错误映射、secret reference 和有限
fallback。表中的“契约测试”表示 request/response、鉴权头、结构化输出、只读
Tool 和错误映射已通过本地协议测试；“disposable live”表示通过受控本地环境访问
真实 endpoint，不等同生产认证。

| Provider | 协议族 | Streaming | Tools | Structured output | Embeddings | 实现成熟度 | 真实 endpoint 资格 |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| OpenAI-compatible | OpenAI Chat Completions-compatible | 是 | 是 | 是 | 是 | adapter 与 transport 已实现并完成契约测试 | 本地 Ollama profile 已通过 disposable live；不代表任意兼容 endpoint 已认证 |
| Anthropic | Anthropic Messages | 是 | 是 | 是 | 否 | adapter、鉴权与 transport 已实现并完成契约测试 | 未做真实 endpoint 认证 |
| Gemini | Google Gemini | 是 | 是 | 是 | 是 | adapter、鉴权与 transport 已实现并完成契约测试 | 未做真实 endpoint 认证 |
| AWS Bedrock | Bedrock Converse | 是 | 是 | 是 | 取决于模型 | adapter、SigV4 与 transport 已实现并完成契约测试 | 未做真实 endpoint 认证 |
| DeepSeek | Responses API / OpenAI-compatible / Anthropic-compatible | 是 | 是 | 是 | 否 | profile、adapter、语义 SSE、取消与有限 fallback 已实现并完成契约测试 | `deepseek-v4-flash` Responses API 已通过凭据门控的 disposable live 诊断与 fallback 资格 |
| 智谱 GLM | OpenAI-compatible / GLM | 是 | 是 | 是 | 是 | profile、adapter 与 transport 已实现并完成契约测试 | 未做真实 endpoint 认证 |
| Kimi / Moonshot | OpenAI-compatible，含可选 MFJS capability | 是 | 是 | 是 | 否 | profile、adapter 与 transport 已实现并完成契约测试 | 未做真实 endpoint 认证 |
| 本地模型 | OpenAI-compatible | 取决于运行时 | 取决于模型 | 取决于模型 | 取决于模型 | adapter、loopback 约束与本地资格 runner 已实现 | Ollama `qwen2.5:0.5b` 已通过无凭据 disposable live 基础资格 |

所有真实 Provider 凭据都必须通过仓库外 secret file、环境引用或 SecretProvider
注入，不得写入代码、配置、日志、Evidence 或资格报告。DeepSeek live 资格仅使用
脱敏合成 Evidence，模型选择的 Tool 仍须经过固定只读 registry 校验；模型本身不
获得 RocketMQ、Executor 或 Execution Agent 的 mutation authority。

当前没有任何 Provider 被仓库声明为生产认证。生产凭据、数据驻留、网络出口、
持续负载、配额/成本、故障转移策略和运维交接仍需部署环境单独认证。机器可校验的
当前成熟度与限制以
[`implementation-status.v1.json`](../config/implementation/implementation-status.v1.json)
为准。
