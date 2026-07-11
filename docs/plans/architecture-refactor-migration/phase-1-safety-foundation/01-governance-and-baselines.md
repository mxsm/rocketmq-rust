# M01：治理、依赖策略与可重复基线

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 1：安全性与基础治理 |
| 状态 | 已完成（2026-07-11） |
| 预计周期 | 1–2 周 |
| 工作包 | WP01 `arc-mut-freeze`、WP06 `dependency-policy` |
| 前置条件 | 设计基线 `f545d638` 与迁移复核基线 `6d152248` 可读取；根 workspace 当前为 22 package |
| 可并行项 | 基线采集子任务可并行；policy 与 baseline 文件由同一 Developer 串行写入 |
| 完成后解锁 | M02、M03 |

## 目标

- 把目标 32-package DAG、禁止边、完整 Client allowlist 和兼容 burn-down 变成机器可验证 policy。
- 冻结新的 `ArcMut` 类型用法以及安全 `mut_from_ref`/`AsMut`/`DerefMut` 逃逸，并为存量逐项指定 owner。
- 固化 API、wire、storage、feature、runtime、性能与 standalone consumer 的可重复基线。
- 建立架构例外必须经 ADR 和 Human Architect 批准的流程。

## 非目标

- 本里程碑不创建 10 个新 crate，不迁移源类型，不修复存量 ArcMut。
- 不把当前依赖图误写成目标图，也不以 baseline 永久豁免新债务。
- 不修改 wire/storage/public API，不调整 Cargo feature 默认值。

## 入口条件

- [x] `[ARCH]` 对照设计 5.3–5.9 冻结目标 DAG、feature owner 和禁止边。
- [x] `[TEST]` 记录根与所有 standalone 项目的验证入口，不运行未完成变更的全量测试。
- [x] `[DEV]` 保存 `git status --short`，确认现有用户改动不与 scripts/CI/文档重叠。
- [x] `[HUMAN]` 确认 Client allowlist 只有 proxy-cluster、admin-core/client_adapter、rocketmq-example。

## 交付物

| 类型 | 交付物 |
|---|---|
| Policy | `scripts/architecture-dependency-policy.json`：目标层级、10 个新 crate、禁边、Client allowlist、facade ledger |
| Guard | `scripts/architecture_dependency_guard.py`：消费 `cargo metadata` 和源码 import，支持 baseline/target 两种模式 |
| ArcMut | `scripts/arc-mut-baseline.json`、`scripts/arc_mut_guard.py`、owner ledger |
| 基线 | 22-package metadata、feature closure、Client 8 个直接消费者、public API/wire/storage/runtime/performance 索引 |
| CI | guard 的正向 job 与故意违规 fixture；不以宽松 lint 替代失败 |
| 文档 | policy schema、例外 ADR 模板、证据目录和 baseline 更新规则 |

## PR 级开发步骤

### PR-M01-01：冻结仓库事实和验证路由

- [x] `[ARCH]` 列出 22 个当前 package 和 32 个目标 package，明确 standalone 不计入 32。
- [x] `[TEST]` 从根/局部 `AGENTS.md` 与 manifest 生成 consumer-validation 表，注明触发条件。
- [x] `[DEV]` 运行 `cargo metadata --format-version 1 --no-deps`，把规范化摘要和命令写入 evidence index；原始生成物保存在 `target/`。
- [x] `[DEV]` 用 manifest 与源码 import 两条路径盘点完整 Client 当前直接消费者，不只依赖目录名称推断。
- [x] `[REV]` 核对基线数字、HEAD、工具链和证据 hash；发现差异时先修正文档，不先编码 policy。
- [x] 回滚点：此 PR 只增加基线资料，删除新增资料即可回滚，不改变构建。

### PR-M01-02：实现依赖 policy 与 guard

- [x] `[ARCH]` 把设计 5.3 的允许依赖、明确禁边和 5.4 DAG 编码为版本化 policy schema。
- [x] `[DEV]` 实现 baseline mode：允许已登记存量边但拒绝新增；实现 target mode：要求 10 个新 crate 和最终 allowlist。
- [x] `[DEV]` 同时检查 normal manifest closure 和源码 import；test/dev dependency 独立报告，不能冒充 production closure。
- [x] `[DEV]` 为 `protocol → transport`、`store-api → backend`、`proxy-local → client`、foundation → facade 等违规各加最小 fixture。
- [x] `[TEST]` 证明 clean fixture 退出 0、每个违规 fixture 非 0，并保存错误信息快照。
- [x] `[REV]` 检查 guard 不依赖目录字符串猜测 package identity，错误必须指出 caller、target、edge kind 和 policy rule。
- [x] 回滚点：guard 先以独立 CI job 落地；若误报阻塞，回滚该 job 和脚本，不改目标 policy 决策。

### PR-M01-03：实现 ArcMut usage guard

- [x] `[ARCH]` 定义检测范围：类型构造/别名、`mut_from_ref`、clone 后可用的安全 `AsMut`/`DerefMut`、`SyncUnsafeCellWrapper`。
- [x] `[DEV]` 生成逐文件、逐符号、owner、计划退出里程碑的 baseline；新增计数必须为零，存量只能下降。
- [x] `[DEV]` guard 区分生产、测试和兼容入口，但三者都报告；不得用改名或 re-export 绕过。
- [x] `[TEST]` 增加新增 usage、删除 usage、移动 usage、baseline 过期四类 fixture。
- [x] `[REV]` 检查 baseline 无永久通配豁免；每个例外必须有 owner、原因、移除里程碑和 ADR。
- [x] 回滚点：若解析器在 Windows/Unix 路径上不一致，回滚 CI 接线但保留已验证的 owner ledger。

### PR-M01-04：兼容与性能基线索引

- [x] `[ARCH]` 冻结 canonical/legacy public path、wire code/header/Serde、20B CQ/Index、feature 默认值的基线清单。
- [x] `[TEST]` 为后续 golden/API diff 定义输入 corpus、规范化方式和失败阈值。
- [x] `[DEV]` 记录现有 runtime audit、error hygiene、RocksDB、MCP 和 standalone validation 命令，不创建不存在的性能/kind guard。
- [x] `[DEV]` 定义性能 profile 元数据 schema和 `target/architecture-refactor/` 证据布局。
- [x] `[HUMAN]` 批准兼容面和性能通用阈值：固定 profile 下吞吐、p99、RSS 任一关键指标不恶化超过 5%。
- [x] 回滚点：基线变更必须恢复到上一份已签名 hash；不得直接覆盖历史证据。

### PR-M01-05：CI、AGENTS 路由和例外流程

- [x] `[DEV]` 将 dependency/ArcMut guard 接入相关 CI，并同步 Windows/Unix 可执行路径。
- [x] `[DEV]` 若修改 `AGENTS.md`、workflow 或 routing script，保持 PowerShell/Bash 路由检查一致。
- [x] `[ARCH]` 发布例外 ADR 规则：范围、owner、风险、到期里程碑、替代方案、批准者。
- [x] `[TEST]` 在 clean checkout 运行 guard 和 routing check。
- [x] `[REV]` 检查没有通过 `allow_failure`、忽略退出码或扩大 baseline 让 job 虚假通过。
- [x] `[HUMAN]` 批准 M01 Gate。

## 公共兼容面

- 本里程碑不改变任何 Rust public API、wire/storage schema 或 feature 默认值。
- policy 把它们声明为受保护面，但不会把当前非法依赖合法化为长期设计。
- baseline 更新属于架构变更；扩大 baseline 必须有 ADR 和 Human Architect 批准，普通修复只能减少计数。

## 验证命令

### 当前即可执行

```powershell
cargo metadata --format-version 1 --no-deps
cargo tree -e normal
rg -n "rocketmq[_-]client|rocketmq_client" --glob "Cargo.toml" --glob "*.rs"
rg -n "ArcMut|mut_from_ref|SyncUnsafeCellWrapper|impl.*AsMut|impl.*DerefMut" --glob "*.rs"
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-agents-routing.ps1
git diff --check
```

### 本里程碑新增后执行

```powershell
python scripts/architecture_dependency_guard.py --mode baseline
python scripts/architecture_dependency_guard.py --mode target --allow-missing-planned-crates
python scripts/arc_mut_guard.py
python scripts/architecture_dependency_guard.py --fixtures
python scripts/arc_mut_guard.py --fixtures
```

目标模式在新 crate 尚未创建时必须通过明确的 `--allow-missing-planned-crates` 报告“计划未完成”，不能伪报目标态已达成；M09 移除该参数。

## 回滚触发器

- guard 在相同 `cargo metadata` 上跨平台产生不同结论。
- clean baseline 被误判，或违规 fixture 未被拒绝。
- CI 接线改变正常构建/feature 语义，或让既有 standalone 项目被误加到根 workspace。
- baseline 数字无法追溯到命令、提交和 hash。

回滚时保留已验证的事实清单，撤销有问题的 guard/CI 接线，修复解析器后以新 PR 重启；不得扩大 policy 来消除误报。

## Exit Checklist

- [x] `[ARCH]` 当前 22-package 与目标 32-package 图均已签名。
- [x] `[DEV]` dependency policy 可检查无环、禁边、Client allowlist 和 facade ledger。
- [x] `[DEV]` ArcMut guard 禁止新增且能证明存量下降。
- [x] `[TEST]` 正向/负向 fixture 在 Windows CI 和主 CI 环境结果一致。
- [x] `[REV]` 没有永久通配例外或静默忽略退出码。
- [x] `[HUMAN]` 公共兼容基线、5% 性能阈值和例外流程已批准。
- [x] 所有当前可执行命令有退出码与证据；未来 guard 只有在文件实际落地后才记为通过。

## 交接物

- 向 M02 交付 ArcMut/runtime/error baseline、owner ledger 和现有 audit 触发规则。
- 向 M03 交付 foundation 禁边、Client 类型迁移清单和 observability closure 基线。
- 向 M09 交付 target mode、32-package/Client allowlist 的最终验收规则。
