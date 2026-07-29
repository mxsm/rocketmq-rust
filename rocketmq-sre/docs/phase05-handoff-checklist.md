# Phase 5 新工程师交接 Checklist

本清单用于把 RocketMQ-Rust AI SRE 的日常操作、安全边界、扩展方式和恢复流程交接给新工程师。
完成者应实际执行代表性流程，并在末尾记录结果；仅阅读文档不算完成。

## 1. 项目和职责边界

- [ ] 已阅读根目录与 `rocketmq-sre/AGENTS.md`。
- [ ] 能说明 Root、MCP、SRE 三个 Cargo workspace 的独立边界和各自 lockfile。
- [ ] 知道普通 RocketMQ Dashboard 与 AI SRE UI 相互独立。
- [ ] 知道 Connector 只通过 MCP Streamable HTTP 接入，不导入 MCP server DTO。
- [ ] 知道 PostgreSQL 保存控制面状态，Docker Compose 是日常环境，Kind 是 Kubernetes 验收环境。
- [ ] 知道 UI 当前优先桌面全屏，移动端适配不在本阶段范围内。

## 2. 环境和访问

- [ ] Rust、Node.js、Docker、Kind 和 `kubectl` 版本满足 README 要求。
- [ ] 已确认 `ROCKETMQ_SRE_TEST_DATABASE_URL` 指向隔离测试数据库。
- [ ] 已从 SecretProvider 或本地开发 fixture 获得 secret reference，未把明文 secret 写入配置或日志。
- [ ] 已确认 OAuth2 audience、scope、tenant 和 cluster allowlist。
- [ ] 已确认当前 Kind context 是
      `kubernetes-admin@rocketmq-sre-phase00`，且脚本使用独立 kubeconfig。
- [ ] 已确认 D: 和 G: 盘有足够空间，Cargo target 不写入源码目录。

## 3. 启动和只读检查

- [ ] 使用 Docker 启动 PostgreSQL，并通过 `pg_isready`。
- [ ] 启动 Control Plane、Connector 和 UI，确认 `/healthz` 与 `/readyz`。
- [ ] 打开集群、Fleet、Incident、Integration 和 Release 页面。
- [ ] 完成一次集群 onboarding、handshake、capability 查看和 offboard。
- [ ] 通过 MCP 查询集群概览、Topic、Broker runtime 和 Consumer lag。
- [ ] 能区分 `ReadyReadOnly`、`ReadOnlyDegraded`、`Rejected` 和 `Offboarded`。
- [ ] 确认 UI 没有未经治理的 RocketMQ mutation、raw apply、DELETE、reset 或 truncate 入口。

## 4. Incident、治理和发布

- [ ] 创建并查看一个 Incident，检查 Evidence、Hypothesis、DiagnosticPack 和审计事件。
- [ ] 能说明模型输出不能绕过 deterministic deny 或直接触发执行。
- [ ] 能说明 R2 必须经过 Policy、异构 Critic、人类审批、短期 Grant、lease/fence。
- [ ] 能说明 R3 动作固定不可达。
- [ ] 查看一个 Fleet release，确认区域串行批次和 cluster-level 状态。
- [ ] 演练 deny、回归、pause 和 rollback，并核对 append-only release events。
- [ ] 验证未知版本、digest drift 或能力缺失进入降级或拒绝状态。

## 5. Enterprise Integration

- [ ] 检查 ITSM、ChatOps、Pager、Email、CMDB、GitOps 和 CI/CD descriptor。
- [ ] 验证 inbound 事件的签名、时间窗、nonce、tenant scope 和 external ID。
- [ ] 验证 outbound 使用 outbox、幂等键、有限重试、dead letter 和人工 replay。
- [ ] 确认 CMDB、GitOps、CI/CD 不携带 repository credential。
- [ ] 确认 CI/CD 只能触发只读 readiness，不能直达 Execution Agent。
- [ ] 完成一次 integration disable 或 secret reference rotate，并核对审计记录。

## 6. 观测、安全和数据处理

- [ ] 确认 Evidence、日志和模型出站内容没有 token、secret、私钥、ACL/TLS material 或消息正文。
- [ ] 能解释 `missing`、`not_production_verified`、`partial` 和 `truncated`，且缺失数据不伪装成 `0`。
- [ ] 查询 MCP Runtime 与 Observability System Resource，确认认证、审计和脱敏。
- [ ] 检查 Prometheus、Loki 和 Tempo 的代表性指标、日志与 trace。
- [ ] 停止 Collector 后验证 RocketMQ 数据面和 MCP 查询不被阻塞。
- [ ] 恢复 Collector 后确认 exporter 状态恢复。

## 7. 备份、恢复和灾难恢复

- [ ] 执行：

  ```powershell
  .\rocketmq-sre\scripts\phase05-control-plane-restore.ps1
  ```

- [ ] 核对 migration、cluster、incident 和 fleet release 数量一致。
- [ ] 确认恢复数据库上的当前 Control Plane `/healthz` 和 `/readyz` 正常。
- [ ] 执行：

  ```powershell
  .\rocketmq-sre\scripts\phase05-test-cluster-dr.ps1 `
    -Kubeconfig G:\rocketmq-sre-phase2-temp\kind-access\rocketmq-sre-phase00.kubeconfig
  ```

- [ ] 核对 Broker Pod UID 已变化，重建后 send/consume/query 均成功。
- [ ] 确认 Control Plane PostgreSQL 状态在测试集群恢复后仍可读。
- [ ] 理解当前 Kind Broker 使用 `emptyDir`：该演练证明组件重建与控制面恢复，不证明历史消息恢复。
- [ ] 如 Broker 重建后的第一次探针处于瞬时启动窗口，确认脚本只进行一次有界重试并记录该事实。

## 8. 扩展和验证

- [ ] 阅读 `docs/phase05-extension-guide.md`，能选择 Provider、Evidence、DiagnosticPack、
      Integration、Action 或 Fleet 的正确扩展点。
- [ ] 新 Rust 模块使用 Rust 2024 文件布局，没有 `mod.rs`。
- [ ] 运行 source layout、OpenAPI 和 UI 类型一致性检查。
- [ ] 运行 focused tests、Clippy、workspace tests 和 UI lint/test/build。
- [ ] 涉及运行时所有权时运行 runtime audit。
- [ ] 涉及边界、Manifest、AGENTS 或 CI 路由时运行 routing drift control。
- [ ] 执行完整企业场景：

  ```powershell
  .\rocketmq-sre\scripts\phase05-enterprise-smoke.ps1 `
    -Kubeconfig G:\rocketmq-sre-phase2-temp\kind-access\rocketmq-sre-phase00.kubeconfig
  ```

- [ ] 对照 `docs/phase05-enterprise-validation-record.md` 检查本次结果，没有把降级或重试隐藏为成功。

## 9. 交接签字

| 项目 | 记录 |
| --- | --- |
| 接手工程师 |  |
| 交接工程师 |  |
| 日期 |  |
| Git revision |  |
| Docker/Kind 环境 |  |
| 完整 smoke 结果 |  |
| 已知限制或后续事项 |  |

完成本清单后，新工程师应能独立执行：环境启动、只读 onboarding、Incident 检查、Fleet 发布演练、
Enterprise Integration 验证、Control Plane 恢复、测试集群 DR 和完整 Phase 5 smoke。
