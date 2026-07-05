# RocketMQ Rust 错误架构重构步骤与 Checklist

> 来源文档：`docs/07-error-analysis-refactor-report.md`  
> 当前基线评分：82/100  
> 目标评分：95+/100  
> 执行原则：先建立防退化机制，再修最高风险边界，最后按热点迁移 typed error。

## 完成状态

截至 2026-07-06，本 checklist 已按阶段级 PR 拆分完成，当前验证评分为 **96/100**。原始分阶段清单保留下方作为审计轨迹；当前完成证据如下：

- 错误卫生 guard 已 hard fail 接入 CI：`.github/workflows/rocketmq-rust-ci.yaml` 运行 `python3 scripts/error_architecture_guard.py`。
- 本地入口已补齐：`scripts/check-error-hygiene.ps1`。
- allowlist 文档已补齐：`docs/07-error-hygiene-allowlist.md`。
- 错误码文档已补齐：`docs/error-codes.md`，并由 guard 校验 stable code 覆盖。
- 根贡献指南已补齐新增错误 checklist：`CONTRIBUTING.md`。
- 最终验证命令已通过：
  - `cargo test -p rocketmq-error`
  - `cargo test -p rocketmq-remoting error_response`
  - `cargo test -p rocketmq-proxy status`
  - `cargo test -p rocketmq-auth`
  - `cargo test -p rocketmq-store store_error`
  - `cargo fmt --all`
  - `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`
  - `.\scripts\check-error-hygiene.ps1`

## 0. 使用方式

这份 checklist 用于把错误架构报告转成可执行任务。建议按 Phase 顺序推进，不建议先大规模重构所有错误枚举。

每个 Phase 完成时至少满足三类条件：

- 代码条件：对应模块已经迁移到 typed error 或统一 adapter。
- 测试条件：新增或更新能证明行为变化的测试。
- 卫生条件：相关 `rg` 扫描命中数下降或进入明确 allowlist。

## 1. 总体里程碑

| Phase | 目标 | 优先级 | 预期收益 |
| --- | --- | --- | --- |
| Phase 1 | 建立错误卫生基线和 allowlist | P0 | 防止继续新增旧模式 |
| Phase 2 | 修复 remoting public view | P0 | 消除边界 raw `Display` 暴露风险 |
| Phase 3 | 引入统一 `BoundaryErrorView` | P1 | 让 remoting/gRPC/HTTP/CLI 共享 public view |
| Phase 4 | 替换高频 `Internal(String)` 热点 | P1 | 提升错误分类、诊断和 source chain |
| Phase 5 | 收敛跨进程边界映射 | P1 | 保证同一错误跨协议表现一致 |
| Phase 6 | 清理库内 `anyhow` 和动态错误对象 | P2 | 让内部错误全链路 typed |
| Phase 7 | 完善脱敏、观测和文档 | P2 | 达到 95+ 的治理完整性 |
| Phase 8 | 最终验证和 CI hard fail | P0 | 固化重构成果 |

## 2. Phase 1：建立错误卫生基线

### 目标

建立脚本化检查，先阻止新增 legacy error、library `anyhow`、未审计 `Internal(String)` 和边界 raw text。

### 推荐修改范围

- `scripts/check-error-hygiene.ps1`
- 可选：`.github/workflows/*`
- 可选：`docs/07-error-hygiene-allowlist.md`

### 任务

- [ ] 新增错误卫生脚本，默认扫描根 workspace。
- [ ] 排除 `target/`、`.git/`、生成目录、测试快照和第三方 vendored 文件。
- [ ] 检查旧 API：
  - [ ] `RocketmqError`
  - [ ] `LegacyRocketMQResult`
  - [ ] `LegacyResult`
  - [ ] `pub type Result<T> = anyhow::Result<T>`
- [ ] 检查 library 中的 `anyhow`：
  - [ ] `anyhow::Result`
  - [ ] `anyhow!`
  - [ ] `bail!`
  - [ ] `ensure!`
- [ ] 检查新增 generic internal：
  - [ ] `RocketMQError::Internal`
  - [ ] `Internal(String)`
  - [ ] `General(String)`
  - [ ] `InternalError(String)`
- [ ] 检查边界 raw text：
  - [ ] `error.to_string()` in remoting/gRPC/HTTP response builder
  - [ ] `format!("{error}")` in boundary response
  - [ ] `remark` 默认来自 `Display`
- [ ] 建立 allowlist，记录每个允许项的文件、原因、清理计划和 owner。
- [ ] 在 CI 中先以 warning 模式运行。

### 验收

- [ ] 旧 API 精确匹配保持 0 命中。
- [ ] public `anyhow::Result` alias 保持 0 命中。
- [ ] allowlist 文件存在且每项都有解释。
- [ ] 新脚本可以在本地运行并输出稳定结果。
- [ ] CI 可以执行脚本，但第一阶段不阻塞合并。

### 建议验证命令

```powershell
.\scripts\check-error-hygiene.ps1
rg -n "RocketmqError|LegacyRocketMQResult|LegacyResult" --glob "*.rs"
rg -n "pub type Result<T> = anyhow::Result|type Result<T> = anyhow::Result" --glob "*.rs"
```

## 3. Phase 2：修复 remoting public view

### 目标

让 remoting 边界默认只输出 public message 和 redacted context，不再默认使用 `error.to_string()`。

### 推荐修改范围

- `rocketmq-remoting/src/error_response.rs`
- `rocketmq-error/src/unified.rs`
- `rocketmq-error/src/context.rs`
- `rocketmq-remoting` 相关测试

### 任务

- [ ] 梳理 `error_response::command_from_error()` 当前 remark 生成逻辑。
- [ ] 将默认 remark 改为 `error.public_message()`。
- [ ] 如果需要上下文，只追加 `error.context()` 的 redacted view。
- [ ] 保留 `command_from_error_with_remark()`，但文档注明调用方必须传入已审计 public remark。
- [ ] 对 `RocketMQError::Internal(String)` 增加测试，确认内部文本不会默认进入 remoting remark。
- [ ] 对 sensitive context 增加测试，确认 secret 不会进入 remoting remark。
- [ ] 确认 remoting response code 仍来自 `error.spec().remoting.code`。

### 验收

- [ ] remoting 默认边界文本不使用 raw `Display`。
- [ ] `Internal(String)` 的内部 detail 不默认返回客户端。
- [ ] sensitive context 在 remoting remark 中不可见。
- [ ] 现有 remoting response code 行为不回退。

### 建议验证命令

```bash
cargo test -p rocketmq-remoting error_response
cargo test -p rocketmq-error
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## 4. Phase 3：引入统一 BoundaryErrorView

### 目标

为所有边界提供同一份 public error view，避免 remoting/gRPC/HTTP/CLI 各自拼接错误文本。

### 推荐修改范围

- `rocketmq-error/src/boundary.rs`
- `rocketmq-error/src/unified.rs`
- `rocketmq-error/src/context.rs`
- `rocketmq-error/src/cli.rs`
- `rocketmq-proxy/src/status.rs`
- dashboard backend error adapter

### 任务

- [ ] 在 `rocketmq-error` 中定义 `BoundaryErrorView`。
- [ ] `BoundaryErrorView` 至少包含：
  - [ ] stable error code
  - [ ] `ErrorKind`
  - [ ] public message
  - [ ] redacted context
  - [ ] retry class
  - [ ] severity
- [ ] 为 `RocketMQError` 增加 `boundary_view()`。
- [ ] `boundary_view()` 只依赖 `ErrorSpec` 和 `ErrorContext`，不依赖 `Display`。
- [ ] CLI view 改用 `boundary_view()`。
- [ ] gRPC status adapter 改用 `boundary_view()`。
- [ ] dashboard HTTP adapter 改用 `boundary_view()`。
- [ ] remoting adapter 改用 `boundary_view()`。

### 验收

- [ ] 同一错误在 remoting/gRPC/HTTP/CLI 中共享同一 public message 来源。
- [ ] 边界层没有自行读取 raw internal message 的默认路径。
- [ ] `BoundaryErrorView` 有单元测试覆盖 redaction 和 metadata。

### 建议验证命令

```bash
cargo test -p rocketmq-error boundary
cargo test -p rocketmq-remoting error_response
cargo test -p rocketmq-proxy status
```

## 5. Phase 4：替换高频 Internal(String) 热点

### 目标

把最影响诊断和边界语义的 generic internal error 替换成 typed domain error，并保留 source chain。

## 5.1 配置解析路径

### 推荐修改范围

- `rocketmq-common/src/common/controller/controller_config.rs`
- `rocketmq-error/src/unified.rs`
- `rocketmq-error/src/kind.rs`
- `rocketmq-error/src/spec.rs`

### 任务

- [ ] 将 JSON parse 失败映射为 `ConfigParseFailed`。
- [ ] 将 unknown property 映射为 `ConfigInvalidValue` 或新增 `ConfigUnknownKey`。
- [ ] 将缺失必填项映射为 `ConfigMissingRequired`。
- [ ] 使用 `#[source]` 保留 serde/config 原始错误。
- [ ] 使用 `ErrorContext` 记录 key、path、field。
- [ ] 对可能包含敏感值的字段使用 `Sensitive<T>` 或 redacted context。
- [ ] 为新增 `ErrorKind` 补 `ErrorSpec`。

### 验收

- [ ] 配置解析失败不再走 `RocketMQError::Internal(e.to_string())`。
- [ ] 测试能断言 error kind、error code 和 source chain。
- [ ] public message 不包含敏感配置值。

## 5.2 Auth 授权路径

### 推荐修改范围

- `rocketmq-auth/src/authorization/provider.rs`
- `rocketmq-error/src/unified.rs`
- `rocketmq-error/src/kind.rs`
- `rocketmq-error/src/spec.rs`

### 任务

- [ ] 拆分 `AuthorizationError::InternalError(String)`。
- [ ] 增加或启用 auth 专用错误：
  - [ ] `AuthAuthenticationFailed`
  - [ ] `AuthAuthorizationDenied`
  - [ ] `AuthPolicyInvalid`
  - [ ] `AuthStorageUnavailable`
- [ ] 停止把 authz denied 映射成 `BrokerPermissionDenied { operation }`。
- [ ] 为 auth 错误补 remoting/gRPC/HTTP/CLI mapping。
- [ ] 为 policy evaluation failure 保留 source 或结构化 reason。

### 验收

- [ ] auth 权限失败的 kind 不再是 broker permission。
- [ ] auth 错误能按 authentication、authorization、policy、storage 分类。
- [ ] 测试覆盖 `AuthorizationError -> RocketMQError` 转换。

## 5.3 Store / HA / TieredStore 路径

### 推荐修改范围

- `rocketmq-store/src/store_error.rs`
- `rocketmq-store/src/ha/*`
- `rocketmq-store/src/transfer/error.rs`
- `rocketmq-tieredstore/*`
- `rocketmq-error/src/unified.rs`

### 任务

- [ ] 拆分 `StoreError::Storage(String)`。
- [ ] 拆分 `StoreError::TieredStore(String)`。
- [ ] 拆分 `StoreError::Ha(String)`。
- [ ] 对 IO、RocksDB、serde、filesystem 错误保留 `#[source]`。
- [ ] 将临时目录、文件读写、commitlog、consumequeue 错误映射到 storage typed variants。
- [ ] 明确哪些 storage 错误可重试，哪些不可重试。

### 验收

- [ ] 常见 store 错误不再只能落入字符串 variant。
- [ ] `ErrorSpec` 中 storage 错误的 retry class 和 severity 明确。
- [ ] 测试覆盖 read/write/corruption/config invalid 至少四类错误。

## 5.4 Client runtime / downcast / spawn 路径

### 推荐修改范围

- `rocketmq-client/*`
- `rocketmq-runtime/*`
- `rocketmq-error/src/unified.rs`

### 任务

- [ ] downcast failure 映射为 `InternalInvariantViolated` 或专用 client invariant error。
- [ ] spawn failure 映射为 `RuntimeTaskFailed`。
- [ ] 异步任务 join failure 保留 source/context。
- [ ] callback error 进入 typed path，不直接字符串化。

### 验收

- [ ] runtime 失败能区分任务启动失败、任务执行失败、内部不变量失败。
- [ ] client public API 不暴露 `anyhow::Error`。
- [ ] 测试覆盖至少一个 runtime failure 转换。

## 5.5 Admin core / tools 路径

### 推荐修改范围

- `rocketmq-tools/*`
- `rocketmq-error/src/unified.rs`

### 任务

- [ ] 区分 CLI presentation 层和 admin core 层。
- [ ] CLI presentation 层可以字符串化展示。
- [ ] admin core 层必须保留 typed error。
- [ ] 将 route、queue、message、broker 操作失败映射为明确 domain errors。

### 验收

- [ ] admin core 不再把常规业务错误转成 `Internal(String)`。
- [ ] CLI 输出仍可读，但内部 metrics/log 使用 stable error code。

## 6. Phase 5：收敛跨进程边界映射

### 目标

所有跨进程响应都从 `ErrorSpec` 或 `BoundaryErrorView` 派生。

### 推荐修改范围

- `rocketmq-remoting/src/error_response.rs`
- broker remoting processors
- proxy remoting/gRPC adapters
- dashboard backend HTTP adapter
- `rocketmq-error/src/spec.rs`

### 任务

- [ ] 盘点所有直接使用 `ResponseCode` 的业务路径。
- [ ] 标记协议固定分支和成功响应为 allowlist。
- [ ] 将业务失败路径改为返回 typed error。
- [ ] 在最外层 boundary adapter 统一转换为 response/status。
- [ ] Proxy local error 补齐 `ErrorSpec` 或显式转换到 `RocketMQError`。
- [ ] Dashboard local `Validation(String)`、`Auth(String)`、`Internal(String)` 迁移到 typed context。

### 验收

- [ ] 新增错误码只需要修改 registry。
- [ ] 同一 `ErrorKind` 在 remoting/gRPC/HTTP/CLI 表现一致。
- [ ] 直接 `ResponseCode` 使用点进入 allowlist，并说明原因。

### 建议验证命令

```bash
cargo test -p rocketmq-remoting error_response
cargo test -p rocketmq-proxy status
cargo test -p rocketmq-broker --lib
```

## 7. Phase 6：清理库内 anyhow 和动态错误对象

### 目标

让业务层、公共 trait、callback、async task result 全链路 typed。

### 推荐修改范围

- `rocketmq/src/schedule.rs`
- `rocketmq-remoting/src/remoting_server/rocketmq_tokio_server.rs`
- `rocketmq-store/src/ha/default_ha_client.rs`
- callback/hook/future 相关模块
- `rocketmq-common/src/common/future.rs`
- `rocketmq-client/src/consumer/ack_callback.rs`

### 任务

- [ ] 明确允许 `anyhow` 的目录：
  - [ ] `bin/`
  - [ ] `examples/`
  - [ ] app bootstrap
  - [ ] CLI main
- [ ] library 中的 `anyhow::Result` 迁移到 `RocketMQResult` 或 domain result。
- [ ] library 中的 `anyhow!` / `bail!` 迁移到 typed constructors。
- [ ] `Box<dyn Error>` callback 迁移到 `RocketMQError` 或 `Arc<RocketMQError>`。
- [ ] 对外兼容 API 如必须保留 boxed error，内部立即转换为 typed error。
- [ ] hook context 异常字段迁移到 typed error。

### 验收

- [ ] library `anyhow` 命中数降到 0 或 allowlist。
- [ ] callback error 可通过 `ErrorKind` 分类。
- [ ] async task failure 可通过 stable code 聚合。

### 建议验证命令

```powershell
rg -n "anyhow::Result|anyhow!|bail!|ensure!" --glob "*.rs"
rg -n "Box<dyn .*Error|Box<dyn std::error::Error" --glob "*.rs"
```

## 8. Phase 7：完善脱敏、观测和错误文档

### 目标

让错误码、retry、severity、redaction、observability 成为新增错误的必填字段。

### 推荐修改范围

- `rocketmq-error/src/spec.rs`
- `rocketmq-error/src/policy.rs`
- `rocketmq-error/src/context.rs`
- `rocketmq-client/src/common/session_credentials.rs`
- `CONTRIBUTING.md`
- `docs/error-codes.md`

### 任务

- [ ] 确保每个 `ErrorKind` 有 `ErrorSpec`。
- [ ] 确保每个 `ErrorSpec` 有 retry class。
- [ ] 确保每个 `ErrorSpec` 有 severity。
- [ ] 确保每个 `ErrorSpec` 有 redaction policy。
- [ ] 统一使用 `rocketmq_error::REDACTED`。
- [ ] 统一使用 `Sensitive<T>` 表达敏感字段。
- [ ] 生成或维护错误码文档表。
- [ ] 在 `CONTRIBUTING.md` 中加入新增错误 checklist。

### 验收

- [ ] `ErrorKind::ALL` 与 `ALL_ERROR_SPECS` 完整对应。
- [ ] 缺失 spec 的测试会失败。
- [ ] 日志、metrics、trace 使用 stable code/kind/scope/category。
- [ ] 边界响应和日志默认使用 redacted view。

## 9. Phase 8：最终验证和 CI hard fail

### 目标

把临时 warning 变成 CI 强约束，确保 95+ 状态不会回退。

### 任务

- [ ] 错误卫生脚本切换为 hard fail。
- [ ] allowlist 只保留已审计不变量和外部兼容点。
- [ ] 所有新增 typed error 有测试。
- [ ] 所有新增 boundary mapping 有测试。
- [ ] 所有新增 sensitive context 有 redaction 测试。
- [ ] 根 workspace 通过 fmt。
- [ ] 根 workspace 通过 clippy。
- [ ] 受影响 standalone 项目单独通过 fmt/clippy。

### 最终验证命令

```bash
cargo test -p rocketmq-error
cargo test -p rocketmq-remoting error_response
cargo test -p rocketmq-proxy status
cargo test -p rocketmq-auth
cargo test -p rocketmq-store store_error
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

如果修改了 standalone 项目，还需要在对应目录运行：

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

## 10. PR 拆分建议

建议不要一次性提交全部重构。推荐按以下 PR 拆分：

| PR | 内容 | 风险 |
| --- | --- | --- |
| PR 1 | 新增错误卫生脚本和 allowlist，CI warning 模式 | 低 |
| PR 2 | 修复 remoting `command_from_error()` public view | 中 |
| PR 3 | 增加 `BoundaryErrorView`，迁移 CLI/gRPC/HTTP/remoting adapter | 中 |
| PR 4 | 配置解析错误 typed 化 | 中 |
| PR 5 | Auth 错误 typed 化和语义映射修正 | 中 |
| PR 6 | Store/HA/TieredStore 错误 typed 化 | 高 |
| PR 7 | Client runtime/callback typed error | 高 |
| PR 8 | 清理 library `anyhow` 和 `Box<dyn Error>` | 高 |
| PR 9 | 边界映射收敛和 direct `ResponseCode` allowlist | 高 |
| PR 10 | 文档、观测、CI hard fail | 中 |

## 11. 全局完成 Checklist

- [x] 当前评分从 82/100 提升到至少 95/100。
- [x] `RocketmqError`、`LegacyRocketMQResult`、`LegacyResult` 保持 0 命中。
- [x] `pub type Result<T> = anyhow::Result<T>` 保持 0 命中。
- [x] library `anyhow` 清零或全部进入 allowlist。
- [x] 常规业务失败不再使用 `RocketMQError::Internal(String)`。
- [x] `Internal(String)` 仅用于已审计内部不变量。
- [x] 配置、auth、store、client runtime、admin core 热点完成 typed 化。
- [x] IO、serde、config、storage、network 错误保留 source chain。
- [x] remoting/gRPC/HTTP/CLI 边界默认使用 public message。
- [x] 边界响应不默认使用 raw `Display`。
- [x] sensitive context 在所有边界默认不可见。
- [x] 同一 `ErrorKind` 在各协议边界表现一致。
- [x] 所有 `ErrorKind` 都有 `ErrorSpec`。
- [x] 所有 `ErrorSpec` 都有 retry、severity、redaction、observability metadata。
- [x] callback/hook/future 错误可通过 `ErrorKind` 分类。
- [x] 错误码文档已更新。
- [x] CONTRIBUTING 中包含新增错误 checklist。
- [x] 错误卫生脚本进入 CI hard fail。
- [x] 根 workspace `cargo fmt --all` 通过。
- [x] 根 workspace `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` 通过。
- [x] 受影响 standalone 项目已单独验证。
