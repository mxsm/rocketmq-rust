# M09：Facade 收口与 32-Package Phase 2 Gate

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 已批准，等待 M04–M08 |
| 预计周期 | 1–2 周 |
| 工作包 | 汇总 WP06–WP13、WP15、WP17–WP19 的目标态证据 |
| 前置条件 | 10 个新 crate 均已落地；所有临时迁移例外有结论 |
| 可并行项 | API/feature/dependency/consumer evidence 可并行收集；最终快照和 Gate 串行冻结 |
| 完成后解锁 | M10、M11 |

## 目标

- 将根 workspace 精确收口为 32 package，并证明目标 DAG 无环、禁边为零。
- 证明 common/remoting/store/proxy/rocketmq-rust 只承担批准的 facade/composition/legacy 职责。
- 完成 R0 兼容发布证据、R1 consumer 迁移清单和下一 major 删除窗口。
- 在同一冻结快照上完成 Reviewer 与 Tester 的独立 Phase 2 验收。

## 非目标

- 不在 closeout PR 中继续迁移大批源码或改变 feature 默认值。
- 不删除尚未跨一个 major 的 deprecated public path。
- 不提前宣称 Phase 3 durability/performance/security/cloud 目标已完成。

## 入口条件

- [ ] `[ARCH]` 逐 crate 核对目标 owner、canonical path、facade ledger 和待删除版本。
- [ ] `[TEST]` 固定最终快照的根/feature/standalone/compatibility 验证矩阵。
- [ ] `[DEV]` 停止功能开发，只修复 Gate 阻塞；每次修复产生新快照。
- [ ] `[HUMAN]` 确认所有 public/feature/format 差异都有明确批准或归零。

## 交付物

| 类型 | 交付物 |
|---|---|
| Workspace | 精确 32-package metadata 和目标 DAG 报告 |
| Dependency | 10 个新 crate 禁边、Client allowlist、proxy/store/observability closure |
| Compatibility | canonical/legacy API compile fixture、wire/storage golden、feature/default diff |
| Facade | common/remoting/store/proxy/legacy 的保留职责、存量边、owner、移除版本 ledger |
| Release | R0 发布顺序、R1 consumer 迁移、下一 major 删除/feature 迁移说明 |
| Review | 同一 Git 快照上的 Architect/Reviewer/Tester/Human Gate 记录 |

## PR 级开发步骤

### PR-M09-01：Workspace 与目标 DAG 收口

- [ ] `[DEV]` 运行 metadata，断言根 workspace package 数恰为 32，standalone 没有误加入。
- [ ] `[DEV]` dependency guard 切到严格 target mode，移除 `--allow-missing-planned-crates` 和全部到期临时例外。
- [ ] `[TEST]` 故意违规 fixture仍能拒绝 foundation→facade、protocol→transport、store-api→backend、proxy-local→client 等边。
- [ ] `[REV]` 人工复核 guard 报告与 `cargo tree -e normal`，确认无环和无 re-export 绕行。
- [ ] 回滚点：closeout 不回滚已完成迁移；失败返回原里程碑修复并重新冻结快照。

### PR-M09-02：Facade 与 legacy purity 审查

- [ ] `[ARCH]` 为 common/remoting/store/proxy/rocketmq-rust 列出允许保留的 public path、composition 和真正通用工具。
- [ ] `[DEV]` 删除误留在 facade 的新算法/owner 状态，把实现归 canonical crate；不删除兼容入口。
- [ ] `[REV]` 检查 facade 只做精确 re-export、config conversion、factory/composition、legacy forwarding。
- [ ] `[TEST]` canonical/legacy path编译和运行差分，验证 old path 仍可用。
- [ ] 回滚点：若纯化影响 public surface，恢复 forwarding adapter并将删除移到下一 major，不恢复算法复制。

### PR-M09-03：Public API、feature、wire/storage 兼容证明

- [ ] `[TEST]` 运行 public API snapshot/diff；任何差异分类为 additive、deprecated 或未批准 breaking。
- [ ] `[TEST]` 运行 protocol simd、transport TLS/observability、store 精确矩阵、admin no-default/adapter/legacy、Proxy R0 矩阵。
- [ ] `[TEST]` 运行 wire JSON/binary、20B CQ/Index、CommitLog/Rocks 和 canonical/legacy golden。
- [ ] `[REV]` 检查 default/no-default 与 R0 设计一致，下一 major feature 未提前启用。
- [ ] `[HUMAN]` 逐项签署允许的 additive/deprecated diff；未批准 breaking 必须修复。

### PR-M09-04：Client allowlist 与跨项目验证

- [ ] `[DEV]` strict guard 断言完整 Client 只在 proxy-cluster、admin-core/client_adapter 和 standalone example。
- [ ] `[TEST]` 根 workspace 和受影响 Example、Tauri、Web backend/frontend 按最近 AGENTS 执行验证；只有 dashboard-common 实际变化时才条件验证 GPUI。
- [ ] `[REV]` 检查 MCP/Dashboard 只经 admin-core，Broker/NameServer/core/local 的 normal closure清零。
- [ ] `[TEST]` MCP deny-by-default、planning no-mutation、streamable HTTP auth/stdout/redaction contract 不变。
- [ ] 回滚点：跨项目失败返回 M07/M08 修复；不通过扩大 allowlist解决。

### PR-M09-05：R0/R1/下一 major 发布包

- [ ] `[ARCH]` 按 model/error/runtime/security/store-api → protocol/observability → transport/auth/local/tiered → rocks → facade → service/tool 固定发布拓扑。
- [ ] `[DEV]` 生成 R0 release note：新 crate、canonical path、deprecated path、无行为变化声明和回滚方式。
- [ ] `[DEV]` 生成 R1 consumer list和 CI 禁止新 facade 边规则；记录外部用量采集方式。
- [ ] `[ARCH]` 生成下一 major 删除列表：admin legacy、common compat、remoting 深路径、Proxy optional mode feature。
- [ ] `[HUMAN]` 批准版本窗口和外部通知，不在代码中提前删除。

### PR-M09-06：冻结快照并执行 Phase 2 Gate

- [ ] `[DEV]` 提交候选快照说明和 evidence index，释放 writer lease。
- [ ] `[REV]` 独立审查同一快照，输出 dependency/API/compat/lifecycle findings。
- [ ] `[TEST]` 独立运行完整矩阵，输出命令、退出码、环境、hash 和 skipped 原因。
- [ ] 任一修复返回同一 `[DEV]`；修复后更新快照并重跑受影响 Review/Test。
- [ ] `[ARCH]` 确认设计到实现追踪完整。
- [ ] `[HUMAN]` 四方结论无未解决 material finding 后批准 Gate。

## 公共兼容面

- R0 只新增 canonical API 和 deprecated re-export/adapter，不删除 public item。
- R1 迁仓内消费者并禁止新增 facade 依赖，外部旧路径继续可编译。
- 下一 major 才删除已公告且满足用量门槛的路径，并切换 Proxy optional mode feature。
- `rocketmq-store` composition 可长期存在；其他 facade 是否长期保留依据外部用量，不按目录美观强删。

## 验证命令

### 当前与新 crate 落地后共同执行

```powershell
cargo metadata --format-version 1 --no-deps
cargo test -p rocketmq-mcp --all-features
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-error-hygiene.ps1
.\scripts\check-agents-routing.ps1
git diff --check
```

另累积执行 M03–M08 中所有实际受影响的 package、精确 feature、RocksDB、MCP 和 standalone 命令；MCP 累计 Gate 明确包含 `cargo test -p rocketmq-mcp --all-features`。GPUI 仅在 dashboard-common 实际变化时进入条件路由。

### 本阶段已交付后严格执行

```powershell
python scripts/architecture_dependency_guard.py --mode target
python scripts/arc_mut_guard.py
```

若 public API snapshot 工具在 M01 选择后有确定命令，使用已签名的实际工具和参数；不得在此文档假定仓库已有未落地的 API guard。

## 回滚触发器

- workspace 不是精确 32 package，或 standalone 被误纳入。
- strict target guard 仍需要临时例外，DAG 有环，Client allowlist未达标。
- old path/feature/wire/storage golden 失败或出现未批准 breaking diff。
- facade 仍拥有新算法/生命周期 owner，或低层通过 facade反向依赖。
- Reviewer/Tester 针对最终快照有未解决 material finding。

M09 本身不通过大范围回滚解决：把问题路由回产生它的 M03–M08 里程碑，由 Developer修复并重新冻结。发生 wire/storage durability 破坏时立即停止发布。

## Exit Checklist

- [ ] `[DEV]` `cargo metadata` 精确报告 32 个根 workspace package。
- [ ] `[REV]` 10 个新 crate 的禁边为零，目标 DAG 无环。
- [ ] `[REV]` Client allowlist精确为 workspace 2 + standalone 1。
- [ ] `[TEST]` API/feature/wire/storage/canonical-legacy/standalone 矩阵全部成功或有获批的环境性跳过。
- [ ] `[REV]` common/remoting/store/proxy/legacy 只保批准职责，ledger 只降不增。
- [ ] `[ARCH]` R0/R1/下一 major 发布与删除计划完整。
- [ ] `[REV]` 与 `[TEST]` 结论针对同一冻结快照。
- [ ] `[HUMAN]` Phase 2 Gate 已签署，无未解决 material finding。

## 交接物

- 向 M10 交付稳定 32-package snapshot、storage/network baseline、compatibility corpus 和性能 profile。
- 向 M11 交付 observability/security/runtime hooks、facade/ArcMut剩余 ledger和部署 consumer清单。
- 向发布负责人交付 R0/R1/下一 major 版本包和回滚索引。
