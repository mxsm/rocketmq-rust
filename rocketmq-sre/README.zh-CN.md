# RocketMQ Rust AI SRE

[English](README.md) | [简体中文](README.zh-CN.md)

`rocketmq-sre` 是面向 RocketMQ Rust 的独立 Rust workspace 和 AI SRE
桌面端运维界面，用于构建 AI 辅助、默认只读的运维平面。Phase 00
提供安全工程基础；Phase 01 增加持久化运维工作流、8 个确定性
DiagnosticPack、受限的多 Provider Model Gateway、Evidence 与知识服务、
资产与拓扑清单、巡检、建议、Shadow 评估，以及独立的桌面端 AI SRE
工作区。

Phase 02 按边界清晰的增量交付。P2-01 增加类型化的告警、关联、拓扑、
预测、仿真、就绪度、通知、复盘和行动项契约，以及仅向前演进的
PostgreSQL migration 和自动生成的 Phase 02 OpenAPI/UI 契约。P2-04
增加经过身份认证的 Alertmanager 与 Provider 无关事件接入、基于拓扑的
确定性 Incident 关联、负责人路由、终态安全处理、运维人员备注、事务性
通知，以及基于持久化 SSE 的时间线更新。P2-05 增加确定性的多窗口 SLO
燃尽率和健康评分。P2-06 增加可解释的 7 天/30 天容量与积压预测、季节性
异常和变点提示、确定性 What-if 仿真、升级/容灾就绪度、持久化预测结果，
以及全宽桌面预测工作区。预测和仿真始终只提供建议，不能创建执行请求。

Phase 03 包含不可变执行契约、持久化 PostgreSQL 执行日志、服务端校验的
ActionPlan 创建、确定性策略评估、禁止自审批的人工审批、服务签名的
ApprovalGrant 签发、执行提交、资源隔离管理、按 correlation 查询的 Audit
API，以及自动生成的 Phase 03 OpenAPI/TypeScript 契约。P3-04 增加
fail-closed 的异构 Critic 门禁：R2 计划只有在不同归一化模型系列完成
不可变审查之后，才能进入人工审批；主模型与 Critic 的调用链和 fallback
身份必须能够精确追踪。除非专用 Executor、Execution Agent、Action
descriptor、策略、审批、lease/fence 和类型化 driver 均被显式启用，
否则目标侧执行始终 fail closed。

Phase 05 增加企业级 Fleet、区域路由、接入配额、资产与合规索引、受限
Fleet 巡检、代表性企业集成、发布护航、DR Center、受治理的制品生命周期、
FinOps 和桌面端企业运维界面。规范的 Phase 05 OpenAPI、只读 Rust/TypeScript
客户端和 `rocketmq-sre` 运维 CLI 共用同一个固定的状态、集群、Incident、
巡检和计划读取边界。CLI 可以校验仅保存在本地的 Plan 与 Runbook 草稿，
但不能提交、审批或执行。

## Workspace 边界

该 workspace 不共享 MCP server 的 Rust DTO。Connector 通过 Streamable
HTTP 使用 MCP，并将通过 wire schema 校验的响应转换为规范 Evidence
契约。Executor 和 Execution Agent 默认编译为禁用状态，不能修改目标集群。

workspace 包含以下 11 个 crate：

- `rocketmq-sre-contracts`：版本化 wire 与持久化契约。
- `rocketmq-sre-core`：Incident 协调与扩展注册表。
- `rocketmq-sre-model-gateway`：规范 Model IR 与 Provider descriptor。
- `rocketmq-sre-control-plane`：Control Plane 服务 composition root。
- `rocketmq-sre-connector`：MCP Connector composition root。
- `rocketmq-sre-executor`：受监督的执行、验证、回滚和恢复边界；不持有
  目标 mutation 凭据。
- `rocketmq-sre-execution-agent`：具有持久 fencing 和可选目标 adapter
  的隔离类型化 driver 边界。
- `rocketmq-sre-probe`：身份独立、行为受限的合成探针与验证。
- `rocketmq-sre-eval`：schema 与覆盖率验证工具。
- `rocketmq-sre-client`：只提供状态、集群、Incident、巡检、计划和
  OpenAPI 固定查询的只读 Rust 客户端。
- `rocketmq-sre-cli`：固定的只读运维命令，以及仅在本地进行的类型化
  Plan 和 Runbook 草稿校验。

## 开发

```powershell
python scripts/check_source_layout.py
cargo check --locked --workspace
cargo test --locked --workspace --all-features
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
cargo run --locked -p rocketmq-sre-eval --bin phase3-schema-export
node scripts/generate_phase3_openapi.mjs
node scripts/generate_phase5_openapi.mjs
npm --prefix ui run generate:api
npm --prefix ui run check:api
npm ci --prefix sdk/typescript
npm test --prefix sdk/typescript
cargo run --locked -p rocketmq-sre-cli -- --help
```

workspace 使用 Rust 2024 的现代源码布局：`foo.rs` 作为父模块，子模块
放在 `foo/` 目录中。源码布局检查会拒绝旧式 `foo/mod.rs` 入口。

## 运行 Phase 00 全栈环境

PostgreSQL 运行在 Docker 容器中，不需要在宿主机安装数据库。大型 Evidence
载荷保存在仅挂载到 Control Plane 的私有 `evidence-objects` named volume
中，因此正常重启整个环境或 Control Plane 后，PostgreSQL 引用和对象内容
都会保留。正常服务启动路径不会选择内存对象存储。

```powershell
.\scripts\dev.ps1 -Action Up
.\scripts\phase00-smoke.ps1 -Target Compose
.\scripts\dev.ps1 -Action Down
```

Compose smoke 等待受限探针制造正 Consumer Lag，然后验证恢复消费后 Lag
下降。它会实际查询 Prometheus、Loki 和 Tempo；通过 Connector 数据源报告
校验版本化 MCP runtime 与 observability resource；重启 PostgreSQL 和
Control Plane 以证明接入状态能够持久化；停止 Collector 后验证
RocketMQ/MCP 数据面不会被阻塞，并在恢复 Collector 后检查 exporter
恢复。最后一步会 offboard 固定的开发集群。`Down` 默认保留 PostgreSQL
volume；如需从全新状态重复完整 smoke，请先按照
[本地环境指南](deploy/dev/README.md)删除 Phase 00 volume。

Phase 01 的反向 Connector 通道在非 loopback 环境中只允许 HTTPS。
Compose 和 Kind 使用受约束的 Control Plane proxy 终止 mTLS，使用独立的
server/client 信任根，根据通过校验的证书覆盖 Connector 身份 header，并
隔离 Connector 对 Axum 的直接访问。使用以下命令校验部署契约：

```powershell
.\scripts\verify-mtls-deployment.ps1 -CheckCertificates
```

普通 RocketMQ Dashboard 与 AI SRE UI 明确分离。AI SRE 工作区负责
Incident、巡检、类型化计划、受监督执行跟踪、Fleet、发布、DR、治理、
集成和 FinOps 工作流。它不复用 Dashboard session，也不复用原始 mutation
API；普通资源页面仅通过有作用域的只读上下文和 deep link 协作。UI
面向 1280×720、1440×900 和 1920×1080 的全屏桌面工作区。窄屏布局保持
不崩溃，但当前阶段不设计专用移动端交互。

在 Phase 00 offboard 之前，使用 Compose 环境运行 Phase 01 实时只读链路：

```powershell
.\scripts\phase01-smoke.ps1 -Target Compose -BootstrapProbe
```

该 smoke 与确定性的 8-pack Shadow 套件互补：实时 smoke 验证真实
Connector/MCP/RocketMQ Evidence 链路和持久化产品工作流；Shadow 为每个
Wave A DiagnosticPack 提供 normal、fault 和 missing Evidence 覆盖。

在当前 Kind 环境中运行 Phase 02 运维闭环验收：

```powershell
.\scripts\phase02-operator-loop-smoke.ps1 -Target Kind
```

该 smoke 会创建并运行确定性巡检，将建议提升为 Incident，接入独立的
Alertmanager 事件，执行模型辅助诊断，查询预测，运行只读流量增长仿真，
验证 exactly-once 且已脱敏的通知投递，并持久化可查询 Action Item 的
Postmortem 草稿。仅在本次运行中启用预留的本地 Email 通知 fixture，并在
`finally` 中再次禁用；它不会访问外部通知服务。

相关文档：

- [项目边界](docs/decisions/0001-project-boundaries.md)
- [MCP 只读边界](docs/decisions/0002-read-only-mcp-boundary.md)
- [Control Plane Connector 通道 mTLS](docs/control-plane-connector-mtls.md)
- [Connector 传输 ADR](docs/connector-control-plane-transport-adr.md)
- [兼容性](docs/compatibility.md)
- [Phase 01 实时 Kind 验证记录](docs/phase01-live-validation-record.md)
- [Phase 02 契约与持久化](docs/phase02-contracts-and-persistence.md)
- [Phase 02 诊断 Evidence 数据源](docs/phase02-evidence-sources.md)
- [Phase 02 DiagnosticPack 目录](docs/phase02-diagnostic-packs.md)
- [Phase 02 告警关联与通知](docs/phase02-alert-correlation.md)
- [Phase 03 执行契约](docs/phase03-execution-contracts.md)
- [Phase 03 PostgreSQL 恢复](docs/phase03-postgres-recovery.md)
- [Phase 03 Plan、Policy、Approval 与 Audit](docs/phase03-plan-policy-approval.md)
- [Phase 03 异构 Critic](docs/phase03-heterogeneous-critic.md)
- [Phase 05 OpenAPI、SDK 与 CLI](docs/phase05-openapi-sdk-cli.md)
- [Phase 05 企业验证记录](docs/phase05-enterprise-validation-record.md)
- [Phase 05 运维指南](docs/phase05-operations-guide.md)
- [Phase 05 扩展指南](docs/phase05-extension-guide.md)
- [Phase 5 新工程师交接 checklist](docs/phase05-handoff-checklist.md)
- [本地环境](deploy/dev/README.md)

## Phase 05 企业级验收

可复现的 Phase 05 验收组合了 100 集群/双区域 PostgreSQL 规模场景、
current/N-1 协议与能力行为、企业集成幂等性、区域 Fleet 发布编排、隔离的
Control Plane 数据库恢复，以及有边界的 Kind Broker 重建练习：

```powershell
.\scripts\phase05-enterprise-smoke.ps1 `
  -Kubeconfig D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig
```

smoke 会把脱敏的机器可读结果写到仓库外。已提交的验证记录明确限定测试
边界：Kind Broker 使用 host-local 持久化 PVC，该练习只证明 Pod 替换时的
消息历史 RPO 0，不代表物理多节点或多区域灾难恢复。current/N-1 检查验证
持久化 runtime handshake 行为；真正运行 N-1 binary 还需要一个此前已发布
的 SRE/MCP release 制品。

受限 soak/Chaos runner 会持续采样全部 SRE、RocketMQ、PostgreSQL 和
observability workload，同时替换 MCP/Connector、Control Plane 和 Broker
Pod，并中断 OTel Collector：

```powershell
# 对 runner 和恢复路径进行短时验证。
.\scripts\phase05-soak-chaos.ps1 `
  -Mode Run `
  -DurationSeconds 90 `
  -SampleIntervalSeconds 5 `
  -CollectorOutageSeconds 3 `
  -InjectFaults

# 6 小时发布验证。
.\scripts\phase05-soak-chaos.ps1 `
  -Mode Run `
  -DurationSeconds 21600 `
  -SampleIntervalSeconds 60 `
  -CollectorOutageSeconds 30 `
  -InjectFaults `
  -FullDurationQualification
```

runner 会拒绝非预期 Kubernetes context，将 kubeconfig 和 Evidence 路径
限制在 D 盘或 F 盘，在 `finally` 中恢复 Collector，保留 Broker PVC UID
集合，并且不记录配置值或 Secret。只有 Evidence 表明运行达到完整持续时间、
4 个故障均已恢复、采样就绪率至少为 99%，且未解决故障集合为空时，完整
验证才会通过。

## Kind 验收

Phase 00 Kind 环境复用仓库固定的 Kubernetes 工具版本、规范 Helm
`dev-single` profile 和本地加载的镜像。它增加临时的集群内 PostgreSQL、
SRE 服务，以及最小 Prometheus/Loki/Tempo/OTel 验收 overlay。该环境不是
生产 Helm 分发包。

```powershell
.\scripts\kind.ps1 -Action Up
.\scripts\kind.ps1 -Action Status
.\scripts\kind.ps1 -Action Smoke
.\scripts\kind.ps1 -Action Down
```

前置条件、固定版本和 smoke 覆盖范围参见
[Kind 验收环境](deploy/kind/README.md)。

## Phase 01 离线 Shadow 评估

Phase 01 evaluator 使用 normal、fault 和 missing-evidence fixture 运行 8 个
Wave A DiagnosticPack。它支持确定性 Mock Provider、仅规则运行，以及
Provider 不可用时 fallback。Compose runner 不允许访问网络；Kind Job
额外禁用 service-account token 和 RBAC，并拒绝全部 ingress/egress。两者
都必须报告 0 次 mutation 和 0 次 Executor 调用。

```powershell
.\scripts\phase01-shadow.ps1 -Target Offline -Provider Mock
.\scripts\phase01-shadow.ps1 -Target Offline -Provider RulesOnly
.\scripts\phase01-shadow.ps1 -Target Offline -Provider Outage
.\scripts\phase01-shadow.ps1 -Target Compose -Provider Mock
```

参见 [Phase 01 Shadow 评估](docs/phase01-shadow-evaluation.md)和
[Phase 01 已知问题与 Phase 02 输入](docs/phase01-known-issues-and-phase02-inputs.md)。
真实 RocketMQ/MCP/Connector/model Provider 的验收 Evidence 记录在
[Phase 01 实时 Kind 验证记录](docs/phase01-live-validation-record.md)中。
