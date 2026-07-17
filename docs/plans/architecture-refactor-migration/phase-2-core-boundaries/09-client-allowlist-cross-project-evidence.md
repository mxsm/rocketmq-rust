# M09-04 Client allowlist 与跨项目消费者收口证据

> 工作包：PR-M09-04
> Issue：#8254
> 基线快照：`5b8b032e68b23a7583e8748ff1e2425e5324f913`
> 快照日期：2026-07-17
> 结论：Client allowlist 精确为 workspace 2 + standalone 1；M09-04 ledger 3 → 0，跨项目矩阵通过。

## 1. Client allowlist 最终结论

完整 `rocketmq-client-rust` 的 manifest 与源码身份均由 strict guard 精确匹配，caller、target、dependency kind、
manifest path、alias、source prefix 任一改变或重复增长都会失败。

| 范围 | caller | manifest 身份 | 允许源码范围 | 理由 |
|---|---|---|---|---|
| workspace | `rocketmq-admin-core` | normal；`rocketmq_client_rust` | `src/client_adapter/` | 唯一 Admin 远程 client lifecycle adapter |
| workspace | `rocketmq-proxy-cluster` | normal；`rocketmq_client_rust` | 整个 Cluster owner crate | 唯一 Proxy Cluster client lifecycle owner |
| standalone | `rocketmq-example` | dev；`rocketmq_client_rust` | `examples/` | 示例程序显式演示 Client API，不进入库生产依赖 |

Broker、NameServer、Proxy Core、Proxy Local、Common 和 Remoting 共享同一 normal-closure 禁止规则，六个 caller
均不得传递到 Client。MCP、Tauri backend、Web backend 不在 allowlist 中，且源码/manifest 均没有直接 Client、
Common 或 Remoting 绕行。

## 2. MCP 三条 M09-04 输入处置

| 输入 | 处置 | 审查依据 |
|---|---|---|
| MCP → Auth | 物理删除 direct optional dependency；`auth` feature 保留为空的稳定 feature 名 | MCP 源码对 `rocketmq-auth` 为零使用；HTTP bearer/JWT 认证由 MCP 本地 guard 实现并由 all-feature 测试覆盖 |
| MCP → Error | 物理删除 | MCP 源码对 `rocketmq-error` 为零使用；自身稳定错误合同由 `McpError`/protocol mapping 持有 |
| MCP → Runtime | 从临时 ledger 提升为目标 DAG 正式边 | `McpApp` 通过 `RuntimeContext` 关闭 owned task，Audit writer 通过 `ServiceContext`/`BlockingExecutor` 执行；删除会违反生命周期和阻塞 I/O 规则 |

处置后 M09-04：3 → 0；总 compatibility/composition ledger 38 → 35，分布为 R1 29、next-major 4、
long-term 2。MCP 的实际 RocketMQ direct dependencies 只剩 Admin Core、Observability 和 Runtime；目标 DAG 还允许
Security API 作为未来窄安全合同，但当前未形成 manifest 边。

MCP、Tauri backend 和 Web backend 均以
`rocketmq-admin-core = { default-features = false, features = ["client-adapter"] }` 到达 Client，不启用
`legacy-common-compat`。这条传递实现边不授予三个 consumer 直接 Client import 权限。

## 3. MCP 安全与协议门禁

| 命令 | 结果 |
|---|---|
| `cargo check -p rocketmq-mcp` | 通过 |
| `cargo test -p rocketmq-mcp` | 72 unit + 2 integration 通过；1 个需要外部集群环境的 E2E ignored |
| `cargo test -p rocketmq-mcp --all-features` | 89 unit + 2 integration 通过；同一外部 E2E ignored |
| `cargo clippy --all-targets -p rocketmq-mcp --features streamable-http -- -D warnings` | 通过 |
| `cargo doc -p rocketmq-mcp --no-deps` | 通过 |

all-features 明确覆盖 streamable HTTP bearer/JWT auth、protected-resource metadata、allowed origin/host、
change-planning no-mutation、runtime opt-in、sensitive output sanitizer 和协议 snapshot。默认 Catalog 仍为只读/诊断，
stdio 仅输出协议帧的既有 integration test 保持通过。

## 4. 跨项目验证

| 项目 | 最近 AGENTS 路由命令 | 结果 |
|---|---|---|
| Example | `cargo fmt --all -- --check`；`cargo clippy --all-targets -- -D warnings` | 通过 |
| Tauri frontend | `npm ci`；`npm run build` | 通过；npm 报告现有 1 moderate/3 high，Vite 报告大 chunk，不改变依赖 |
| Tauri backend | `cargo fmt --all -- --check`；all-target/all-feature strict Clippy | 通过 |
| Web frontend | `npm ci`；`npm run build` | 通过；0 vulnerabilities，Recharts deprecation/大 chunk 为非阻塞提示 |
| Web backend | `cargo fmt --all -- --check`；all-target/all-feature strict Clippy/build | 通过 |
| GPUI | 条件路由未触发 | `rocketmq-dashboard-common` 没有变化，按任务文档不运行 |

Cargo 验证把 Example、Tauri backend、Web backend 三份 standalone lockfile 校准到当前 path dependency：Client
记录 Security API 边，Remoting 记录已移除的 Macros 边。这些是当前可重现闭包的机械 lock 差分，不改变
standalone manifest 或运行行为。

## 5. 架构与根质量门禁

| Gate | 结果 |
|---|---|
| M09-04 + M07/M08/M09 回归合同 | 41/41 通过 |
| Dependency guard 单元/违规 fixtures | 43/43 通过 |
| strict target/baseline dependency guard 与违规 fixtures | 通过；target ledger 35/35、dev-only 3/3、未授权 finding 0 |
| runtime enforcing audit | 通过；MCP Runtime 正式边保持 owned lifecycle/BlockingExecutor |
| AGENTS routing drift control | 通过 |
| 根 workspace exact fmt 与 all-target/all-feature strict Clippy | 通过 |
| `git diff --check` | 通过 |

Windows linker message 明确忽略 `-D warnings`，既有 future-incompatibility、npm audit 与前端 chunk/deprecation
提示均如实记录；没有把非零退出码描述为通过。

补充失败记录：一次组合 guard 命令误引用不存在的
`scripts/tests/run_architecture_dependency_fixtures.py`，因此该组合退出码非零；实际 fixture 入口
`scripts.tests.test_architecture_dependency_guard` 随后 43/43 通过。一次额外的全 `scripts/tests` discover 在
600 秒外层上限后留下本次启动的孤立 Python 扫描进程，已终止且不计为通过；按 M09-04 受影响范围拆分后，
M07/M08/M09 相关合同 41/41 通过。未用拆分结果伪称全 discover 通过。

## 6. 回滚与总体目标衔接

回滚不得扩大 Client allowlist、恢复 MCP 未使用 Auth/Error 边、让 Dashboard 直接 import Client，或把 Audit writer
改为 detached task/裸阻塞 I/O。若 Runtime 正式边被证伪，必须先把 owned lifecycle 能力迁入已批准的窄 owner，
再删除边，而不是把它重新放回过期 M09-04 ledger。

M09-04 完成后，57/82 个工作包完成、25 个未完成；下一工作包为 PR-M09-05。剩余分布为 M09 2、M10 5、
M11 12、M12 6。M09-05 将生成 R0/R1/下一 major 发布包；本工作包不提前宣称 M09 或 Phase 2 Gate 完成。
