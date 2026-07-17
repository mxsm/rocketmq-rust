# M09-02 Facade 与 legacy purity 收口证据

> 工作包：PR-M09-02
> Issue：#8250
> 快照日期：2026-07-17
> 结论：11 条 M09-02 到期输入全部处置，不延期、不新增例外；兼容/组合 ledger 49 → 38。

## 1. 审查结论

本工作包按“精确 re-export、config conversion、factory/composition、legacy forwarding”四类允许职责审查
`rocketmq-common`、`rocketmq-remoting`、`rocketmq-store`、`rocketmq-proxy` 和 `rocketmq-rust`。审查不以
目录名称判断，而以 public path、状态 owner、算法 owner、依赖闭包和运行差分共同判断。

| crate | R0 允许保留的 public path / 职责 | canonical owner | 退出窗口 |
|---|---|---|---|
| `rocketmq-common` | `common::{message,config,filter,sys_flag}` 旧路径；Model/Protocol/Security/Runtime/Transport 精确兼容导出；环境、网络、磁盘、字符串等跨服务工具 | Model、Protocol、Security API、Runtime、Transport；真正跨服务且无领域状态的工具仍由 Common 持有 | 仓内消费者 R1；Protocol 旧导出 next-major |
| `rocketmq-remoting` | `protocol` 深路径、request/response code/header/body；兼容 command/codec；client/server/TLS factory 与生命周期 forwarding | Protocol 持有 wire/schema，Transport 持有 session/admission/client/server；Remoting 只保 R0 组合和转换 | 仓内消费者 R1；外部深路径 next-major |
| `rocketmq-store` | Store API/Local/Rocks/Tiered 的精确导出；config/backend factory；Local/Tiered composition；旧 MessageStore、CommitLog 和生命周期/telemetry adapter | Store API 与三个 backend crate；Store composition 可长期存在 | 仓内旧路径 R1；composition long-term |
| `rocketmq-proxy` | Core/Cluster/Local 精确导出；auth、ingress/config conversion、bootstrap/factory composition 和 R0 ingress adapter | Proxy Core/Cluster/Local 与 Auth | 仓内旧 Common/Remoting 路径 R1；模式 feature next-major |
| `rocketmq-rust` | `ArcMut`、旧 lock/queue/shutdown/task/schedule 路径，以及 Runtime 精确导出；禁止新增生命周期 owner | Runtime；遗留并发容器按既有兼容台账排空 | R1/M10，外部旧路径 next-major |

`rocketmq-store/src/inspection.rs` 是新的窄兼容入口，但只逐项 `pub use` Local 的规范 CommitLog parser，
没有复制解析算法或声明 owner。Store Inspect 的消息 ID 与 `UNIQ_KEY` 投影属于工具展示逻辑，不进入 facade。

## 2. 11 条到期输入的处置

| 输入 | 处置 | 依据 |
|---|---|---|
| Filter → Common | 物理删除；`ExpressionType` 直连 Protocol，`NOW` 使用 Filter 本地无状态 wall-clock | Filter 不再通过 facade 取规范类型或通用时间 |
| Remoting → Macros | 物理删除 | manifest 有边但源码零使用 |
| Store Inspect → Common | 物理删除；经 Store 精确导出调用 Local parser | 工具不再依赖 Common message decoder |
| NameServer → Controller | 纳入目标 DAG | 仅限 `bootstrap.rs` 与 namesrv binary 的内嵌 Controller 组合，没有低层反向边 |
| Proxy → Error / Model | 纳入目标 DAG | 仅用于 facade 错误、状态和结果转换，是组合边而非临时兼容债务 |
| Remoting → Error / Runtime | 纳入目标 DAG | 兼容错误转换与 client/server 生命周期所必需，未形成新 owner |
| Store → Error / Runtime / Observability | 纳入目标 DAG | Store 是已批准可长期存在的 composition；三条边服务错误映射、owned lifecycle 和 telemetry adapter |

处置后 M09-02 removal window 为 0；剩余 38 条为 R1 29、M09-04 3、next-major 4、long-term 2。
目标 DAG 的明确组合边不计作 compatibility debt，但仍受 package/closure/facade purity contract 约束。

## 3. 兼容与行为差分

- Filter 的 SQL92 public behavior 不变；`NOW` 仍为 Unix epoch 毫秒。
- Store Inspect 继续输出 CommitLog message ID 与 client message ID；新增 golden 回归固定 IPv4 host、物理偏移和
  `UNIQ_KEY` 属性的 legacy 输出。
- Store 的 `inspection` path 与 Local canonical parser 具有相同类型身份；facade 只 re-export。
- Proxy、Remoting、Store 和 `rocketmq-rust` 的既有 public path 未删除；M08、M06、M05、M07 contract 继续作为
  canonical/legacy compile 与运行差分。

## 4. Gate 与验证记录

| Gate | 命令 | 结果 |
|---|---|---|
| M09-02 contract | `python -m unittest scripts.tests.test_m09_facade_purity_closeout scripts.tests.test_m09_target_dag_closeout` | 10 项通过 |
| Target DAG | `python scripts/architecture_dependency_guard.py --mode target` | 38/38 ledger，3/3 dev-only，未授权 finding 0 |
| 聚焦行为 | `cargo test -p rocketmq-filter --lib`；`cargo test -p rocketmq-store-inspect` | 103 + 3 项通过 |
| Facade 包级回归 | Remoting、Proxy、`rocketmq-rust` lib tests | 116 + 82 + 33 项通过 |
| Facade 架构回归 | M06/M07/M08 相关合同；全部常规 architecture contract；相关 Store Local owner/adapter 合同 | 44 + 215 + 4 项通过 |
| Dependency guard | baseline、target、6 个 dependency fixture、`cargo tree -e normal --workspace` | 全部退出码 0 |
| 根质量门禁 | `cargo fmt --all -- --check`；`cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；仅有不受 `-D warnings` 控制的 Windows linker message 和既有 future-incompat 提示 |
| 生命周期/路由 | `runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline`；`check-agents-routing.ps1` | 通过 |
| ArcMut 标准门禁 | 默认 baseline guard、24 fixtures、65 项 guard 单测 | 通过；本工作包无新增 ArcMut |
| 文本/依赖树 | `git diff --check`；normal workspace tree 输出到 target evidence | 通过 |

补充审计：`python scripts/arc_mut_guard.py --current-milestone M09` 会因现有 baseline 的
`current_milestone=M05` 以及 821 个早于 M09 的过期条目失败。这不是本工作包引入的增长，默认 guard、fixture
和单测均通过；也没有通过修改 baseline 消除告警。该治理漂移应在后续 ArcMut/R1/M10 收口中单独纠正。

## 5. 回滚边界

若纯化影响 R0 public surface，只恢复指向 canonical owner 的 forwarding adapter；不得恢复 Common decoder 依赖、
重复 CommitLog/SQL 算法或新的生命周期 owner。目标 DAG 的组合边若被证伪，应先把职责移到既有 canonical crate，
再删除边；不得重新登记为过期 M09-02 例外。

## 6. 总体目标衔接

M09-02 完成后，55/82 个工作包完成、27 个未完成；下一工作包为 PR-M09-03。M09-03 将证明 public API、
feature、wire/storage 兼容，本工作包不提前宣称 Phase 2 Gate 或 M09 完成。
