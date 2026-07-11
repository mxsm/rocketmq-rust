# Phase 1 交付记录：安全性与基础治理

## 交付元数据

| 字段 | 值 |
|---|---|
| 里程碑 | M01–M03 |
| 状态 | 已完成，待 GitHub squash merge |
| Issue | [#8180](https://github.com/mxsm/rocketmq-rust/issues/8180) |
| PR | 创建后补录 |
| 分支 | `mxsm/architecture-refactor-safety-foundation` |
| 基线提交 | `70ec1a177b17be3dfa61af50e29337711a0c58e1` |
| 候选提交 | 提交后补录 |
| 完成日期 | 2026-07-11 |
| Developer | Codex 主代理与实现子代理 |
| Reviewer | 独立代码审查子代理 |
| Tester | Codex 根代理验证流水线 |

## 范围与实现

- M01：交付架构依赖 policy/guard、ArcMut 词法与别名守卫、CI 接线、ADR-013 和可审计 owner/deadline 台账。
- M02：交付 typed flush failure、Store health、动态 TaskGroup child lease、owner-scoped bounded pending request、complete-once、单一绝对 shutdown deadline 与有序 Broker drain。
- M03：创建 `rocketmq-model` 与 `rocketmq-security-api`，迁移中立值类型/分配算法，保留精确兼容 re-export，解除 observability 对 common/facade 的闭包，并交付 secure profile dry-run。
- 公共兼容：保留 legacy `MessageStore::flush`、Controller `apply_event`、Remoting header/Channel 构造入口；canonical fallible API 承担新语义。
- ArcMut：冻结 M01 为 1,277 identities / 3,452 occurrences；M03 收口为 1,266 / 3,430。没有新增 identity/occurrence，27 个上下文变化均由 ADR-013 一对一审批。

## 关键验证结果

| 门禁 | 结果 |
|---|---|
| `cargo fmt --all -- --check` | 通过，退出码 0 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过，退出码 0 |
| Runtime/Remoting/Store/Controller/Common/Client/Auth/Model/Security API 库测试 | 通过；包含 Remoting 1,453、Store 621、Client 955、Common 727、Auth 346 |
| Model/Security/兼容身份/Pending/Runtime 模型集成测试 | 通过；Pending 10k timeout、TaskGroup 100k child churn 均通过 |
| CommitLog recovery | 9/9 通过，覆盖正常/异常恢复、dirty tail 与 File/RocksDB restart parity |
| Observability CI feature matrix | 7 个 feature 组合的 check/Clippy/test 共 21 步全部通过 |
| RocksDB specialized gate | 两项 Clippy、82+5 Store tests、20+4 Broker tests 全部通过 |
| RocketMQ MCP specialized gate | check、72 tests（另 2 个可运行集成测试）、streamable-http Clippy、doc 全部通过 |
| `python scripts/arc_mut_guard.py` | 通过；63 单元测试、24 fixtures 通过 |
| `python scripts/architecture_dependency_guard.py` | baseline mode 通过 |
| `runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| `check-agents-routing.ps1` | 通过，4 standalone Cargo、3 Node、8 routes |
| `git diff --check` | 通过 |
| Standalone `rocketmq-example` | fmt/Clippy 通过；清理陈旧本地包增量缓存后复验 |
| Standalone Tauri backend | fmt/全特性 Clippy 通过 |
| Standalone Web backend | fmt/全特性 Clippy/全目标 build 通过 |
| Standalone GPUI | fmt 通过；Clippy 被基线已有的 3 个 `collapsible_if` 阻塞，相关文件与基线提交无差异 |

## 已知基线问题与风险

- `check-error-hygiene.ps1` 本阶段新增问题已清零；命令仍因基线已有的 auth source stringification、MCP `anyhow` allowlist 和缺失的两个 error governance 文档退出 1。列出的源文件与基线提交无差异，两个文档在基线提交不存在。
- GPUI 的 3 个 `collapsible_if` 是独立项目基线问题，Phase 1 未修改相应 UI 文件；不在本阶段越界修复。
- 目标 32-package DAG 的 target mode 在 M04–M09 crate 尚未创建时应报告未完成，不将其记为 Phase 1 通过项。
- 精确 Observability/RocksDB/MCP 门禁产生约 187 GiB 构建产物并触发磁盘耗尽；已按约定执行根 `cargo clean`，释放约 187.1 GiB 后重新完成最终根 Clippy 与行为测试。

## 回滚与交接

- 回滚单位：整个 Phase 1 squash commit；行为修复测试和 ledger 保留为后续重新落地的证据。
- Store：legacy facade 可继续消费最后 durable watermark；canonical typed API 是 M06 的输入。
- Runtime/Remoting：M05/M07 继续消费 `ServiceContext`、child lease、PendingRequestTable 和绝对 deadline；不得恢复 detached/global close 行为。
- Model/Security：M04/M05/M06 使用 canonical crate；旧路径至少在当前兼容窗口内保持精确 re-export。
- Observability：M11 接续 secure default/rotation/semantic registry；Phase 1 dry-run 不改变现有部署默认值。

## 完成结论

- Developer：实现与证据齐全。
- Reviewer：关键高风险问题已修复并复审。
- Tester：最终候选的根门禁与适用专项门禁完成；仅保留上述可证明的基线失败。
- Human：用户已授权按 Phase 创建 Issue、分支、PR，管理员 squash merge 后从最新 `main` 继续下一 Phase。
