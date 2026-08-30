# Phase 5 扩展开发指南

本指南说明如何在不破坏只读、审批、租户隔离和兼容边界的前提下扩展 Provider、Evidence
Source、DiagnosticPack、Integration、治理对象和 UI。

## 1. 先选择扩展类型

| 需求 | 扩展点 | 主要位置 |
| --- | --- | --- |
| 增加大模型 | `ProviderDescriptor` + Model Gateway adapter | `rocketmq-sre-model-gateway` |
| 增加遥测/资产来源 | `EvidenceSourceDescriptor` + Connector source | `rocketmq-sre-connector` |
| 增加诊断能力 | `DiagnosticPackDescriptor` + deterministic evaluator | `rocketmq-sre-core`、Control Plane |
| 增加外部系统 | `IntegrationDescriptor` + typed adapter | Control Plane release/integration 模块 |
| 增加受控动作 | `ActionDescriptor` + Policy + typed Agent driver | Contracts、Executor、Execution Agent |
| 增加 Fleet API | versioned contract + repository/service/API | Control Plane `fleet/` |
| 增加产品页面 | OpenAPI type + API client + desktop page | `rocketmq-ai/rocketmq-sre/ui` |

不要通过复用 MCP server DTO、Dashboard session、raw JSON、任意 URL、shell 或通用 Admin request
实现扩展。

## 2. Rust 2024 源码布局

项目使用 Rust 2024，不创建 `mod.rs`：

```text
src/
  feature.rs
  feature/
    api.rs
    model.rs
    repository.rs
    service.rs
```

`feature.rs` 声明子模块并只导出必要 API。运行：

```powershell
python .\rocketmq-ai\rocketmq-sre\scripts\check_source_layout.py
```

## 3. 公共 Descriptor

所有 Descriptor 保持以下公共字段：

```text
id
version
owner
supported_versions
required_capabilities
config_schema
status
deprecation
```

规则：

- ID 稳定、全局唯一，推荐 `rocketmq-sre.<kind>.<name>.v1`。
- major 表示不兼容变化；同 major 只能增加可选字段。
- 未知 major、未知 required feature、digest drift 必须 fail closed。
- Registry 支持注册、升级、禁用、废弃和回滚。
- 配置 Schema 不包含 secret；只保存 SecretProvider reference。
- 使用 RFC 8785 canonical JSON 和 `sha256:<hex>` digest。

## 4. Model Provider

现有协议族包括 OpenAI-compatible、Anthropic、Gemini、Bedrock、DeepSeek、智谱 GLM、
Kimi/Moonshot 和本地模型。

新增 Provider：

1. 在 Contracts/Model Gateway 定义或复用 `ProviderDescriptor`。
2. 映射到 Canonical Model IR，不让业务模块直接依赖厂商 SDK DTO。
3. 明确 tool calling、structured output、streaming、vision、context 和 residency 能力。
4. 通过 `SecretProvider` 解析 credential reference，禁止日志打印配置对象。
5. 实现 timeout、并发、token/cost budget、重试上限和 circuit breaker。
6. 输出模型、provider、revision 和 invocation lineage。
7. 增加网络隔离的 fixture、当前协议测试、未知能力 fail-closed 测试。

路由顺序保持：健康、能力、驻留、质量优先级，最后才在同级候选中比较成本。

## 5. Evidence Source

新增来源必须输出 Canonical Evidence，而不是任意 JSON：

1. 在 `EvidenceSourceDescriptor` 声明 query、freshness、sensitivity、owner 和 capabilities。
2. 先验证 wire schema，再转换。
3. 限制行数、字节数、时间范围、并发和超时。
4. 缺失信号返回 `missing` 或 `not_production_verified`，不能伪造 `0`。
5. 清理 token、secret、ACL/TLS、消息正文、客户端 IP 和内部地址。
6. 大内容使用受控 reference；Evidence hash 排除 ID、采集时间和 warnings 等非语义字段。
7. 为 source unavailable、partial、truncated、freshness 超期增加测试。

Connector 只通过 MCP Streamable HTTP 黑盒调用 MCP，不导入 MCP server 类型。

## 6. DiagnosticPack

DiagnosticPack 应保持规则优先、证据可解释：

1. 定义适用组件、症状、required/optional signals、missing behavior 和 freshness。
2. 输出 hypothesis、支持证据、反证、置信度和下一步只读查询。
3. 所有列表和 evidence bundle 有界。
4. 模型只能解释或补充候选，不得覆盖 deterministic deny。
5. 诊断结果不能直接创建执行；只能生成 ActionPlan draft。
6. 在 `capability-to-signal-coverage.v1.yaml` 增加映射。
7. 增加 normal、fault、missing 三类 fixture 和 replay 测试。

## 7. Enterprise Integration

选择封闭能力：`InboundEvent`、`OutboundNotification`、`Ticketing`、`OnCall`、`Cmdb`、
`DesiredState`、`ReleaseEvent` 或 `ExternalPolicy`。

实现要求：

- Inbound 使用 typed payload、HMAC-SHA256、RFC 3339 时间窗、nonce 和 external ID。
- Outbound 使用事务 outbox、target-scoped idempotency key、bounded retry、dead letter 和人工 replay。
- ChatOps/Pager/Email 复用 notification outbox；ITSM 使用 integration outbox。
- CMDB/GitOps/CI/CD 是 inbound-only，不携带 repository credential。
- CI/CD 事件只能触发只读 readiness，不能直达 Execution Agent。
- 外部审批仍必须校验内部 scope、plan hash、step-up、有效期和职责分离。
- health、last delivery、disable、secret reference rotate 都要有审计。

参考验证：

```powershell
$env:ROCKETMQ_SRE_TEST_DATABASE_URL =
  'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre'

cargo test `
  --manifest-path .\rocketmq-ai\rocketmq-sre\Cargo.toml `
  --locked `
  -p rocketmq-sre-control-plane `
  release_management::repository_tests::postgres_enterprise_integration_events_are_signed_scoped_and_idempotent `
  -- --ignored --exact
```

## 8. Action 和执行扩展

Action 是风险最高的扩展：

1. 先建立 `ActionDescriptor`，声明资源类型、前置条件、postcondition、rollback 和风险。
2. Safe/Mutating/Dangerous 与 Read/Plan/R1/R2/R3 同时维护。
3. R2 需要 Policy、异构 Critic、人类审批、短期 Grant、lease/fence。
4. R3 固定不可达；不得新增绕过 Agent 的实现。
5. Execution Agent driver 只接受 typed request 和 allowlisted resource。
6. 所有执行先持久化 intent，再 dispatch；结果、verification、rollback append-only。
7. 重试复用 idempotency key，内容不一致返回冲突。
8. 未知 descriptor/version、过期或 quarantined 治理版本 fail closed。

## 9. Fleet、OpenAPI 和 UI

新增 Fleet 功能按 `contract -> migration -> repository -> service -> API -> OpenAPI -> generated
types -> UI` 顺序实现：

```powershell
node .\rocketmq-ai\rocketmq-sre\scripts\generate_phase5_openapi.mjs
npm --prefix .\rocketmq-ai\rocketmq-sre\ui run generate:api
npm --prefix .\rocketmq-ai\rocketmq-sre\ui run check:api
```

API 要求：

- `/v1` versioned contract，稳定 `operationId`；
- tenant、region、cluster scope；
- 有界 `limit/offset` 或 cursor；
- GET 使用 `rocketmq:read`，mutation 使用精确管理 scope；
- 不提供 DELETE、raw apply、reset、truncate；
- mutation body 是 typed request。

UI 基于 React 18、TypeScript、Vite 和 shadcn/Radix 组件，优先桌面全屏。状态必须有文本或图标，
不能只靠颜色；表格、筛选和 URL 查询需要键盘可用。

## 10. 兼容和验证

区域组件注册必须声明 component、protocol、schema digest 和 capabilities：

- current：`full`；
- N-1：`read_only_degraded`；
- 其他版本、digest drift、missing capability：`denied`。

最小验证：

```powershell
cargo fmt --manifest-path .\rocketmq-ai\rocketmq-sre\Cargo.toml --all -- --check
cargo clippy --manifest-path .\rocketmq-ai\rocketmq-sre\Cargo.toml `
  --locked --workspace --all-targets --all-features -- -D warnings
cargo test --manifest-path .\rocketmq-ai\rocketmq-sre\Cargo.toml `
  --locked --workspace --all-features

npm --prefix .\rocketmq-ai\rocketmq-sre\ui run lint
npm --prefix .\rocketmq-ai\rocketmq-sre\ui test -- --run
npm --prefix .\rocketmq-ai\rocketmq-sre\ui run build
```

涉及生产后台任务、TaskGroup、shutdown 或 BlockingExecutor 时，再运行根仓库 runtime audit。
涉及项目边界、Manifest、AGENTS 或 CI 路由时，运行 AGENTS routing drift control。

## 11. 扩展提交 Checklist

- [ ] 没有 `mod.rs`、raw shell、通用 Admin mutation 或 MCP DTO 依赖。
- [ ] Descriptor、Schema、digest、owner、版本和 deprecation 已定义。
- [ ] tenant/region/cluster scope 与错误路径 fail closed。
- [ ] 输出、并发、超时、重试、token/byte budget 有界。
- [ ] secret 只使用 reference，日志和 Evidence 已脱敏。
- [ ] current/N-1/未知版本测试完成。
- [ ] PostgreSQL migration forward-only，append-only/immutable 约束已覆盖。
- [ ] OpenAPI、生成类型和 UI 一致。
- [ ] focused test、Clippy、文档和完整场景验证通过。
