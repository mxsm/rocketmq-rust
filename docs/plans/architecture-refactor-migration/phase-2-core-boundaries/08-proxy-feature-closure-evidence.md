# PR-M08-06 Proxy Feature Closure 与下一 Major Fixture

## 状态与完成定义

- 当前 R0 继续保持 `rocketmq-proxy default = []`，Core、Cluster、Local 为非 optional normal dependency。
- 本证据只验证当前行为并冻结下一 major 预期，不在 R0 manifest 创建 `cluster-mode`、`local-mode` 或
  `compat-all-modes`。
- Client 直接依赖白名单精确保持 workspace 2 + standalone 1；Proxy facade 不在白名单中。
- Core/Local normal closure 不得包含完整 Client；Cluster normal closure 不得包含 Broker、Store 或 Local。

## R0 实际 Feature/Closure 矩阵

| 组合 | 当前依赖/行为预期 | 验证 |
|---|---|---|
| Core default/no-default | 两者 closure 相同；无 Client/Broker/Store/Auth provider/Proxy adapter | `cargo check`、unit、normal tree |
| Cluster default/no-default | 两者 closure 相同；包含 Core + Client；无 Broker/Store/Local | `cargo check`、unit、normal tree |
| Local default/no-default | 两者 closure 相同；包含 Core + Broker；无 Client/Cluster | `cargo check`、unit、normal tree |
| Local + `tieredstore` | 增加 Tiered backend；仍无 Client/Cluster | feature check/test、normal tree |
| Facade default/no-default | 两者继续同时编译 Cluster 与 Local，保持 R0 兼容语义 | check/test、normal tree |
| Facade + `observability` | 保持双 adapter，并启用 observability 转发 | feature check、normal tree |
| Facade + `tieredstore` | 只经 Local 转发 Tiered 能力；R0 Cluster 仍因非 optional 同时编译 | feature check、normal tree |

`cargo tree -e normal` 只用于生产 normal closure。`cargo tree -e dev` 单独报告：Core 与 Cluster 没有 dev edge，
Local 只有外部 `tempfile`，Facade 只有外部 `criterion`、`hex`、`hmac`、`serde_json` 与 `sha1`；没有新增
RocketMQ 内部 dev-only 绕行。

## 下一 Major 预期（不在 R0 启用）

机器可读 fixture：`scripts/fixtures/proxy-next-major-features.toml`。

下一 major 才执行以下破坏性 feature 迁移：

```toml
[features]
default = ["compat-all-modes"]
cluster-mode = ["dep:rocketmq-proxy-cluster"]
local-mode = ["dep:rocketmq-proxy-local"]
compat-all-modes = ["cluster-mode", "local-mode"]
tieredstore = ["local-mode", "rocketmq-proxy-local/tieredstore"]
```

同时把 `rocketmq-proxy-cluster` 与 `rocketmq-proxy-local` 改为 optional。届时：

- `--no-default-features` 只保 composition/Core，不含 Cluster、Local、Client 或 Broker；
- `cluster-mode` 只选择 Cluster/Client，不含 Local/Broker/Store；
- `local-mode` 只选择 Local/Broker，不含 Cluster/Client；
- `compat-all-modes` 保持当前双模式兼容；
- `tieredstore` 必须蕴含 `local-mode`，不得单独启用 Cluster。

这是下一 major 的公开兼容边界，不得在 R0/R1 静默改变 default/no-default 语义。

## Client 与兼容性结论

- policy 的 manifest/source allowlist 均只有 Admin Core adapter、Proxy Cluster 与 standalone Example；历史 Proxy
  facade 临时例外已经删除，target guard 的 Client 分类为零。
- canonical/legacy path、ProxyConfig Serde/env/CLI、gRPC/Remoting ingress、shutdown/fault 行为必须在同一候选
  快照继续通过。
- 全部矩阵已取得成功退出码，M08-06 完成后进度为 53/82，剩余 29 个工作包。

## 最终验证结果

- Core test 47、Cluster test 19、Local default/tiered test 各 8、Facade test 99，所有 feature check 成功。
- `python -m unittest discover -s scripts/tests -p "test_*.py"`：362/362 通过。
- 根 workspace `cargo fmt --all -- --check` 与 32-package all-target/all-feature strict Clippy 通过。
- dependency baseline 通过；target mode 按预期非零并精确为 48（46 direct + 2 transitive），Client 分类为 0。
- ArcMut 实际 guard、runtime enforcing audit、AGENTS routing 与 `git diff --check` 通过。
- typed-error guard 仅复现 main 既有 11 项：Broker source stringification 1、MCP `anyhow` 8、缺失治理文档
  2；本工作包零新增，不将该门禁记为通过。

## 回滚

- fixture 或 closure 合同误报时，只修正测试/事实，不修改 R0 feature 语义规避失败。
- 任一 default/no-default、canonical/legacy、gRPC/Remoting 或 shutdown/fault 回归，返回对应 Core/Cluster/Local/
  facade owner 修复；不恢复 Proxy Client 直边，也不扩大 allowlist。
- 下一 major 迁移必须独立发布、提供公告和 feature diff；本工作包不得把 fixture 复制进当前 Cargo manifest。
