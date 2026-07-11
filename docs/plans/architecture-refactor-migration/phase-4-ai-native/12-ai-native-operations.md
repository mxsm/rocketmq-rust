# M12：AI Native 证据驱动运维

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 4：AI Native 运维 |
| 状态 | 已批准，等待 M10/M11 |
| 预计周期 | 8–12 周 |
| 工作包 | Phase 4 新增；复用 WP14 的进度证据和 WP16 的安全合同 |
| 前置条件 | Telemetry semantic registry、AdminSession、QueryFacade、Evidence/Rules、RBAC/audit和SLO稳定 |
| 可并行项 | KG/RAG/规则/eval可并行；Apply边界必须在Plan验证后单独串行批准 |
| 完成后解锁 | 最终目标态评审 |

## 目标

- 在既有 MCP P2–P4 能力上建设受控 Telemetry Knowledge Graph、RAG和多领域确定性诊断。
- 让LLM只解释证据、补充探索和生成候选计划，不进入数据面、不直接执行命令。
- 保持现有Plan Tool无副作用；未来Apply使用独立feature/catalog/endpoint/tool name和完整控制环。
- 在LLM离线、证据不完整、越权和错误计划场景下保持人工运维与核心服务正常。

## 非目标

- 不索引message body、credential、任意用户payload或未脱敏配置。
- 不让模型执行shell、访问WAL/socket/进程内可变状态或绕过Admin API。
- 不给现有`change-planning` Tool增加mutation语义。

## 入口条件

- [ ] `[ARCH]` 冻结KG实体/关系、RAG来源/ACL/freshness、Evidence ID和Plan/Apply合同。
- [ ] `[TEST]` 建立离线replay corpus、golden diagnosis、越权/注入/错误计划/red-team矩阵。
- [ ] `[DEV]` 确认MCP service/apply目录无用户修改重叠。
- [ ] `[HUMAN]` 单独批准Apply是否进入实施；未批准时M12只交付Plan/诊断。

## 交付物

| 类型 | 交付物 |
|---|---|
| KG | cluster/broker/topic/queue/group/client/task/WAL segment/alert/deployment/config version及受控关系 |
| RAG | 官方文档/runbook/schema/ADR/脱敏incident/metric定义，ACL/tenant/freshness/source可追踪 |
| Diagnosis | lag、under-replication、disk、flush、route、task、auth storm等确定性规则 |
| Plan | evidence、precondition、impact、expiry、idempotency、rollback、approval要求 |
| Apply | 若获批：独立feature/catalog/endpoint，policy/executor/verifier/rollback |
| Eval | replay、partial/missing evidence、LLM offline、越权、提示注入、错误计划、audit overflow |

## PR 级开发步骤

### PR-M12-01：Evidence normalization 与 Knowledge Graph

- [ ] `[ARCH]` 只允许semantic registry登记的实体/属性进入KG，定义版本和有效期。
- [ ] `[DEV]` 实现knowledge_graph模块，数据通过QueryFacade/telemetry接口进入，不直连WAL/socket。
- [ ] `[DEV]` tenant/cluster隔离，所有事实带source/version/observed_at/validity/evidence_id。
- [ ] `[TEST]` replay同一EvidenceSnapshot得到确定图；跨tenant、过期、冲突和缺失证据正确隔离。
- [ ] `[REV]` 检查无body/secret/user payload，图大小、TTL和更新队列有界。
- [ ] 回滚点：关闭KG消费，现有QueryFacade/Diagnosis继续工作。

### PR-M12-02：受控RAG

- [ ] `[DEV]` 建版本匹配的允许语料registry，检索前执行ACL/tenant filter。
- [ ] `[DEV]` 返回source/version/validity window，过期或不匹配版本降低confidence并提示缺证据。
- [ ] `[TEST]` 覆盖跨tenant泄漏、恶意runbook/incident提示注入、无来源回答和版本错配。
- [ ] `[REV]` 无Evidence/source的模型输出只能是探索建议，不能成为动作前提。
- [ ] 回滚点：禁用RAG后确定性规则和人工runbook链接仍可用。

### PR-M12-03：多领域确定性诊断

- [ ] `[DEV]` 扩展versioned server-owned rules，先覆盖lag、under-replication、disk/flush、route stale、task leak、auth denial storm。
- [ ] `[DEV]` 每条结论输出evidence、规则版本、confidence、partial/missing evidence和建议验证。
- [ ] `[TEST]` 用离线snapshot做golden replay；无历史数据时拒绝time-range和caller自定义阈值。
- [ ] `[REV]` LLM只能排序/解释规则假设，不修改server-owned阈值或伪造evidence。
- [ ] 回滚点：按规则version关闭，保留旧consumer-lag规则。

### PR-M12-04：Plan contract 与无副作用证明

- [ ] `[ARCH]` 冻结结构化Plan：precondition、impact、expiry、idempotency key、approval、verify、rollback。
- [ ] `[DEV]` 现有13个Tool合同保持不变，change-planning仍`mutates_cluster: false`。
- [ ] `[TEST]` 对所有Plan路径做副作用spy：无mutation API、无shell、无写admin调用、无credential输出。
- [ ] `[REV]` 检查模型文本不能被解释为可执行命令，计划过期/证据变化后必须重新生成。
- [ ] `[HUMAN]` 先签署Plan Gate，再决定是否启动下一PR。
- [ ] 回滚点：Plan仅输出，关闭新planner不影响现有Tool/Resource。

### PR-M12-05：独立Apply边界（仅在Human批准后）

- [ ] 入口：`[HUMAN]` 对Apply范围和action allowlist给出独立批准；当前manifest没有`dangerous-tools`，未批准前不得创建该feature或执行Apply命令。
- [ ] `[DEV]` 创建未来独立`dangerous-tools` feature、catalog/endpoint和不可混淆tool name；现有Plan不共享mutation dispatcher。
- [ ] `[DEV]` 同时要求compile opt-in、runtime opt-in、RBAC、人工/策略批准、scoped token、rate limit、audit。
- [ ] `[DEV]` executor只接受结构化allowlisted action并走Admin facade；verifier采集post-condition，失败进入rollback。
- [ ] `[TEST]` 覆盖任一门禁缺失、重复idempotency、过期、precondition变化、partial apply、verify失败、rollback失败和audit overflow。
- [ ] `[REV]` 检查deny-by-default、无shell、无WAL/socket、无数据面依赖。
- [ ] `[HUMAN]` 每类action单独批准；未批准action不进入catalog。
- [ ] 回滚点：关闭runtime opt-in/endpoint即停止新Apply；inflight按审计状态Verify/Rollback。

### PR-M12-06：Eval、red-team 与离线fallback

- [ ] `[TEST]` 运行确定性replay、LLM离线/超时、幻觉、提示注入、越权、错误计划、模型供应商故障。
- [ ] `[DEV]` LLM不可用时返回确定性诊断/缺证据，不影响Broker/Client/Store或人工CLI/API。
- [ ] `[REV]` 检查token/成本/并发/队列/超时有界，模型和检索telemetry不含敏感数据。
- [ ] `[ARCH]` 将eval阈值、模型/规则/schema版本和回滚runbook纳入发布证据。
- [ ] `[HUMAN]` threat model/red-team无未解决高风险后批准最终Gate。

## 公共兼容面

- 现有MCP 2025-11-25 typed Catalog、8个默认只读/诊断Tool、5个Plan Tool、Resource/Template URI和错误合同保持。
- Apply不复用/更名现有Plan Tool；未启用独立feature与runtime opt-in时不可发现、不可调用。
- Evidence/Rules/Plan/KG/RAG schema均versioned；旧snapshot可重放或明确拒绝不兼容版本。
- AI故障不改变核心服务、CLI/API或人工运维可用性。

## 验证命令

### 当前即可执行

```powershell
cargo check -p rocketmq-mcp
cargo test -p rocketmq-mcp
cargo test -p rocketmq-mcp --all-features
cargo clippy --all-targets -p rocketmq-mcp --features streamable-http -- -D warnings
cargo doc -p rocketmq-mcp --no-deps
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-error-hygiene.ps1
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
git diff --check
```

### 本里程碑新增后执行

```powershell
cargo test -p rocketmq-mcp knowledge_graph
cargo test -p rocketmq-mcp retrieval
cargo test -p rocketmq-mcp diagnosis
cargo test -p rocketmq-mcp plan_has_no_side_effects
python scripts/architecture_dependency_guard.py --mode target
```

### 未来 Apply 经独立 Human Gate 且 feature 实际交付后执行

```powershell
cargo test -p rocketmq-mcp --features dangerous-tools apply
```

`dangerous-tools` 当前不存在；只有 PR-M12-05 获批并实际新增该feature后，以上未来命令才成为可执行Gate。未批准时证据明确记录“未实施Apply”，不是当前命令失败或跳过。

## 回滚触发器

- KG/RAG泄漏tenant/body/secret，或无来源模型输出进入动作前提。
- Plan产生副作用，Apply可在任一门禁缺失时调用，或模型可执行shell/绕过Admin facade。
- 错误计划无法Verify/Rollback/audit，或AI故障影响数据面/人工运维。
- red-team存在未解决高风险越权、提示注入或credential泄漏。

立即关闭RAG/LLM或Apply runtime opt-in，保留确定性诊断与人工接口；对inflight Apply执行审计驱动的Verify/Rollback并升级`[HUMAN]`。

## Exit Checklist

- [ ] `[REV]` AI/LLM依赖不在Broker/Client/Store数据路径。
- [ ] `[TEST]` KG/RAG tenant、source、freshness、privacy和有界性测试通过。
- [ ] `[TEST]` 多领域规则可确定性重放并正确标记partial/missing evidence。
- [ ] `[TEST]` 所有现有Plan Tool无副作用合同通过。
- [ ] `[REV]` 若Apply存在，四重门禁、RBAC/audit/verify/rollback均fail closed。
- [ ] `[TEST]` LLM离线时核心服务和人工CLI/API正常。
- [ ] `[TEST]` threat model/red-team无未解决高风险。
- [ ] `[HUMAN]` Plan/Apply范围和最终目标态Gate已签署。

## 交接物

- 向运维团队交付KG/RAG来源清单、规则/模型/evidence版本、SLO dashboard和人工fallback runbook。
- 向安全团队交付threat model、red-team、RBAC/action allowlist、audit/rollback证据。
- 向Human Architect交付目标96分各维度的自动化证据索引；未通过的门槛不得计入现状分。
