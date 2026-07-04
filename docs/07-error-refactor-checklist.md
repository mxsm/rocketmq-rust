---
title: "Error 架构重构推进清单"
permalink: /docs/zh/error-refactor-checklist/
excerpt: "RocketMQ Rust error 架构重构未完成项、任务拆分和开发验收 checklist。"
last_modified_at: 2026-07-05T00:00:00+08:00
toc: true
classes: wide
---

# Error 架构重构推进清单

## 当前判断

本清单基于 2026-07-04 对当前仓库的静态扫描和关键文件抽查。

当前项目已经完成了重构中的一批关键基础项：

- `RocketmqError`、`LegacyRocketMQResult`、`LegacyResult` 在 Rust 源码中已清零。
- `rocketmq-error` 已有 `ErrorKind`、`ErrorCode`、`ErrorScope`、`ErrorSpec`、协议 primitive、`RetryClass`、`ObserveSpec`、`Sensitive<T>`。
- `rocketmq-error` 已有 registry、protocol、policy、redaction 合约测试。
- `rocketmq-macros` 已生成 typed `RocketMQError`。
- `rocketmq-remoting` 和 `rocketmq-proxy` 已有部分边界 adapter 接入中心 spec。
- `rocketmq-client` 已有 `retry_decision` 读取 `RetryClass`。
- `SessionCredentials` 的 `Display` / `Debug` 已对 secret、signature、security token 脱敏。

按本清单定义，当前 error 架构重构推进项已经完成。后续新增错误类型、协议出口或 standalone 项目变更，继续通过中心 spec、typed boundary adapter、redaction guard 和 CI guard 维持约束。

## 本次扫描信号

以下计数是架构信号，不等同于 bug 数量；`to_string()`、`anyhow::Result` 和 `dyn Error` 需要结合上下文判断是否允许。

| 区域 | Legacy | `anyhow::Result` | `Internal/General(String)` | `dyn Error` / downcast | spec/policy 使用 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `rocketmq-error` | 0 | 0 | 2 | 0 | 236 |
| `rocketmq-remoting` | 0 | 5 | 0 | 6 | 1 |
| `rocketmq-client` | 0 | 0 | 0 | 47 | 10 |
| `rocketmq-broker` | 0 | 3 | 0 | 9 | 2 |
| `rocketmq-store` | 0 | 5 | 0 | 3 | 70 |
| `rocketmq-auth` | 0 | 0 | 0 | 15 | 6 |
| `rocketmq-controller` | 0 | 2 | 0 | 9 | 6 |
| `rocketmq-namesrv` | 0 | 2 | 0 | 0 | 0 |
| `rocketmq-proxy` | 0 | 0 | 0 | 3 | 2 |
| `rocketmq-common` | 0 | 0 | 0 | 1 | 3 |
| `rocketmq-tools` | 0 | 22 | 0 | 0 | 37 |
| `rocketmq-dashboard-common` | 0 | 10 | 0 | 0 | 0 |
| dashboard web backend | 0 | 7 | 1 | 0 | 0 |
| Tauri backend | 0 | 5 | 0 | 0 | 6 |
| GPUI dashboard | 0 | 1 | 0 | 0 | 0 |
| `rocketmq-example` | 0 | 0 | 0 | 0 | 0 |

## 推进原则

- 每个任务完成后，相关 Cargo project 必须能独立编译并通过 clippy。
- 保持 `rocketmq-error` 低层依赖方向，不让它依赖 remoting、proxy、dashboard 或 tools。
- `anyhow` 只允许在 binary、CLI、测试和一次性工具最外层出现；共享库和公共业务 API 不使用它作为契约。
- 外部协议边界只把中心 spec primitive 转成本地类型，不在边界重新定义错误语义。
- `to_string()` 只能用于最终展示、日志摘要或明确 allowlist，不用于常规 source 链传递。
- 敏感字段默认 redacted；所有例外必须有明确理由和测试。

## 里程碑视图

| 阶段 | 目标 | 当前状态 | 完成标志 |
| --- | --- | --- | --- |
| M1 | 补全 error kernel 合约 | 已有骨架，缺 category/redaction policy 深度集成 | `ErrorSpec` 覆盖 category、redaction、recovery、observe、protocol |
| M2 | 清理 public `anyhow` | core error/common 基本通过，其他共享边界仍有命中 | library/shared API 无未登记 `anyhow::Result` |
| M3 | remoting/broker/namesrv 统一出口 | remoting adapter 已接入，processor Java-compatible 本地 code 已登记 allowlist | processor 通用失败走 `rocketmq_remoting::error_response` 或明确 allowlist |
| M4 | proxy gRPC 完整接入 | 部分使用 `spec().grpc`，仍有本地 override/table | `RocketMQError` 路径优先由中心 grpc spec 派生 |
| M5 | dashboard HTTP 完整接入 | `DashboardError::RocketMq` typed，但 HTTP 映射本地硬编码 | RocketMQ 错误使用 `spec().http` 和 stable code |
| M6 | client callback typed 化 | legacy downcast 已移除，仍有 `dyn Error` + typed downcast | callback 内部事实源改为 `RocketMQError` |
| M7 | domain crate source preservation | store/auth/controller/broker 仍有 source 字符串化 | I/O、serde、rocksdb、raft、runtime source 有 typed 保留或 allowlist |
| M8 | redaction 全仓库收口 | 重点模型已脱敏，未全量使用中心 `Sensitive<T>` | secret/token/signature/password Debug/Display/log 有 guard |
| M9 | CI guard 收口 | `scripts/error_architecture_guard.py` 可通过，但未见 CI 调用 | root CI 和相关 dashboard CI 调用 guard |
| M10 | standalone 验证 | 已全量校验 | 受影响 standalone project 按各自目录 clippy 通过 |

## 任务卡与 Checklist

### T01. 补全 `rocketmq-error` 内核合约

**范围**：`rocketmq-error/src/kind.rs`、`spec.rs`、`context.rs`、`unified.rs`、相关 tests。

**完成状态**：

- `ErrorSpec` 已包含 `kind/code/scope/category/public_message/remoting/grpc/http/cli/recovery/observe/redact`。
- `RocketMQError` 已提供 `public_message()` 和 redaction-aware `context()`，边界输出可避免继续读取 `Display` 作为机器契约。
- `RocketMQError::Internal(String)` 已在 guard 中建立基线路径 allowlist；跨 crate 替换为 typed variant 的收敛继续归 T08。

**追踪**：Issue [#7961](https://github.com/mxsm/rocketmq-rust/issues/7961)，PR [#7962](https://github.com/mxsm/rocketmq-rust/pull/7962)。

**开发 checklist**：

- [x] 新增或确认 `ErrorCategory`，区分 network、storage、protocol、auth、config、system 等低基数分类。
- [x] 在 `ErrorSpec` 中加入 `category`，并为所有 `ErrorKind::ALL` 填充。
- [x] 新增 `RedactionPolicy` 或等价字段，并写入 `ErrorSpec`。
- [x] 明确 `Display`、`Debug`、public message、structured context 的职责边界。
- [x] 为 `RocketMQError` 增加统一 context 访问方式，或给关键 variant 挂载 `ErrorContext`。
- [x] 对 `Internal(String)` 制定 allowlist；能建模的场景改成 typed variant。
- [x] 补齐 registry completeness、category、redaction policy、Display/context 测试。

**验收 checklist**：

- [x] `ALL_ERROR_SPECS.len() == ErrorKind::ALL.len()` 仍通过。
- [x] 每个 `ErrorKind` 都有 code、scope、category、recovery、observe、protocol、redaction。
- [x] 新增错误时漏填任何 spec 字段会测试失败。
- [x] 不新增 public `anyhow` alias。

**建议验证**：

```powershell
cargo test -p rocketmq-error
py scripts\error_architecture_guard.py
```

### T02. 清理 public `anyhow::Result` 和边界 allowlist

**范围**：`rocketmq-remoting`、`rocketmq-broker`、`rocketmq-controller`、`rocketmq-namesrv`、`rocketmq-dashboard-common`、dashboard web backend、`rocketmq-tools`、standalone dashboard。

**完成状态**：

- `rocketmq-remoting/src/protocol/command_custom_header.rs` 的 `check_fields()` 已改为 `RocketMQResult<()>`。
- `rocketmq-dashboard-common` 已建立 `DashboardCommonError` / `DashboardCommonResult<T>`，service/api/proxy/nameserver 共享接口不再暴露 `anyhow::Result`。
- dashboard web backend 的 config/state/service 内部路径已改为 `DashboardError`；`main.rs` / `lib.rs` 作为进程入口层保留 `anyhow` 并进入 allowlist。
- tools TUI 的可复用 form/executor 逻辑已改为 `RocketMQResult`；`main.rs` / terminal runtime 作为 TUI app boundary 保留 `anyhow` 并进入 allowlist。
- `scripts/error_architecture_guard.py` 已新增 `anyhow` allowlist 检查，剩余 `anyhow::Result` / `anyhow::Error` 命中必须位于 binary、test/example/build script 或显式登记的外层 worker/entry boundary。

**追踪**：Issue [#7937](https://github.com/mxsm/rocketmq-rust/issues/7937)，PR [#7938](https://github.com/mxsm/rocketmq-rust/pull/7938)。

**开发 checklist**：

- [x] 建立 `anyhow` allowlist，明确 binary、test、example、CLI outer boundary 可保留。
- [x] 将共享库 API 改为 `RocketMQResult<T>` 或 crate-local typed result。
- [x] 将 `rocketmq-remoting` header validation 改为 typed error。
- [x] 将 `rocketmq-dashboard-common` 的 service/api/proxy/nameserver trait 改为 typed error。
- [x] dashboard web backend 内部服务层避免直接暴露 `anyhow::Result`，入口层可保留。
- [x] tools 中可复用 executor/core 逻辑避免 public `anyhow`；TUI main/run 可保留。
- [x] 扩展 guard，扫描 library/shared API 的 public `anyhow`，并允许配置 allowlist。

**验收 checklist**：

- [x] 根 workspace library API 无未登记 `anyhow::Result`。
- [x] standalone/shared dashboard crate 无未登记 `anyhow::Result`。
- [x] 保留的 `anyhow` 全部有 allowlist 说明。

**建议验证**：

```powershell
rg -n "anyhow::Result" --glob "*.rs"
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
py scripts\error_architecture_guard.py
cd rocketmq-dashboard\rocketmq-dashboard-web\backend
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
```

### T03. 统一 remoting、broker、namesrv processor 错误出口

**范围**：`rocketmq-remoting/src/error_response.rs`、`rocketmq-remoting/src/remoting.rs`、`rocketmq-broker/src/processor*`、`rocketmq-namesrv/src/processor*`。

**完成状态**：

- `rocketmq-remoting/src/error_response.rs` 已提供基于 `error.spec().remoting` 的统一 response helper，覆盖 typed error、unsupported request code、internal fallback 和 opaque 保留。
- broker fast-failure fallback 与 auth-admin 通用失败映射已改为通过 remoting adapter 输出；仅保留 `UserNotExist` 这类 Java-compatible 本地业务 response code。
- namesrv processor 中通用 `SystemError`、`InvalidParameter`、`NoPermission`、`QueryNotFound` 路径已迁移到 typed error 或统一 adapter。
- `scripts/error_architecture_guard.py` 已增加 processor generic response allowlist，禁止新增未登记的通用 response code 构造。

**追踪**：Issue [#7939](https://github.com/mxsm/rocketmq-rust/issues/7939)，PR [#7940](https://github.com/mxsm/rocketmq-rust/pull/7940)。

**开发 checklist**：

- [x] 给 `rocketmq_remoting::error_response` 增加统一 helper：typed error、unsupported request code、internal/system fallback、opaque 保留。
- [x] 先迁移通用失败路径：unsupported request、decode/header validation、serialization、auth/config error。
- [x] broker processor 中业务特定成功/空结果 code 保留本地，异常失败走 adapter。
- [x] namesrv processor 中通用 `SystemError`、`NoPermission`、`QueryNotFound` 逐步改为 typed error + adapter。
- [x] 删除或缩小本地 `RocketMQError -> ResponseCode` match。
- [x] 给 broker/namesrv 添加 mapper tests，验证典型 `ErrorKind` 到 `ResponseCode`。
- [x] 扩展 `error_architecture_guard.py`，禁止 processor 新增未 allowlist 的通用错误构造。

**验收 checklist**：

- [x] processor 不再手写通用 `RequestCodeNotSupported`。
- [x] 通用 `SystemError` fallback 有明确 allowlist。
- [x] 每个对外 typed failure 都能由 `ErrorSpec.remoting` 得到 response code。

**建议验证**：

```powershell
cargo test -p rocketmq-remoting
cargo test -p rocketmq-broker
cargo test -p rocketmq-namesrv
py scripts\error_architecture_guard.py
```

### T04. 收敛 proxy gRPC 映射

**范围**：`rocketmq-proxy/src/status.rs`、`rocketmq-proxy/src/error.rs`、相关 gRPC tests。

**完成状态**：

- `ProxyError::RocketMQ` 默认 payload/status 已由 `ErrorSpec.grpc` 派生，payload message 使用 `RocketMQError::public_message()`，不再依赖 `Display` 文本。
- broker response code 兼容路径已收敛为显式 `broker_response_payload_override` allowlist，并同时声明 payload code 与 tonic status。
- proxy local-only 错误已建立 `ProxyErrorKind`，本地映射集中在 `local_error_grpc_mapping`。
- 旧的 `tonic_code_from_payload_code` 旁路 helper 和 display-string topic-route 推断已移除，并由 `scripts/error_architecture_guard.py` 阻止回退。

**追踪**：Issue [#7941](https://github.com/mxsm/rocketmq-rust/issues/7941)，PR [#7942](https://github.com/mxsm/rocketmq-rust/pull/7942)。

**开发 checklist**：

- [x] 区分 `RocketMQError` 通用路径与 proxy local-only 错误路径。
- [x] `RocketMQError` 默认 payload/status 从 `ErrorSpec.grpc` 派生。
- [x] 只保留必须基于 broker response code 的 override，并写清 allowlist。
- [x] 为 proxy local-only error 建立 `ProxyErrorKind` 或转换到 `RocketMQError`。
- [x] 避免新增旁路 gRPC 映射表。
- [x] 补齐 `v2::Code` 与 `tonic::Code` 的 focused tests。

**验收 checklist**：

- [x] `RocketMQError` gRPC 输出不依赖 display string。
- [x] override 分支都有测试和注释说明。
- [x] 新增 proxy error 不会绕过中心 spec 或 local-only allowlist。

**建议验证**：

```powershell
cargo test -p rocketmq-proxy
rg -n "v2::Code::|tonic::Code|rocketmq_payload_override" rocketmq-proxy/src/status.rs
```

### T05. 接入 dashboard HTTP boundary adapter

**范围**：`rocketmq-dashboard/rocketmq-dashboard-web/backend/src/error/dashboard_error.rs`、`rocketmq-dashboard-common`、dashboard backend service/config/admin 层。

**完成状态**：

- `DashboardError::RocketMq` 的 HTTP status 已由 `error.spec().http.status` 派生，API code 已使用 `error.spec().code.as_str()`。
- HTTP response body 已改为 `RocketMQError::public_message()` + redacted context，不再使用 `Display` 作为外部契约。
- local `Validation/Config/Auth/NotFound/Internal` 保留稳定 code/status；配置与内部 source error 通过 `ConfigSource/InternalSource` 保留 source，但响应体只暴露安全消息。
- 配置读写、SQLite 配置、monitor 配置与 dashboard task manager 的底层错误文本不再直接拼接进 HTTP error body。
- `scripts/error_architecture_guard.py` 已加入 dashboard HTTP boundary 回归检查，阻止回退到 `BAD_GATEWAY/ROCKETMQ_ERROR/self.to_string()` 路径。

**追踪**：Issue [#7943](https://github.com/mxsm/rocketmq-rust/issues/7943)，PR [#7944](https://github.com/mxsm/rocketmq-rust/pull/7944)。

**开发 checklist**：

- [x] 对 `DashboardError::RocketMq(error)` 使用 `error.spec().http.status` 生成 HTTP status。
- [x] API code 使用 `error.spec().code.as_str()`，或明确加 dashboard prefix。
- [x] response message 使用 `spec.public_message` + redacted context，避免泄露内部 detail。
- [x] 对 local `Validation/Config/Auth/NotFound/Internal` 建立稳定 code。
- [x] 配置读写、监控配置、admin client 的 `format!("...{error}")` 路径改成 typed source 或 redacted message。
- [x] 增加 dashboard backend HTTP error mapping tests。

**验收 checklist**：

- [x] `RocketMQError::route_not_found` 等典型错误映射到正确 HTTP status/code。
- [x] dashboard API 不用 display text 作为机器契约。
- [x] secret/token/signature 不出现在 error body。

**建议验证**：

```powershell
cd rocketmq-dashboard\rocketmq-dashboard-web\backend
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
```

### T06. client callback 和 request future typed 化

**范围**：`rocketmq-client/src/consumer/pull_callback.rs`、`pop_callback.rs`、producer/consumer callback API、相关 tests。

**完成状态**：

- `PullCallback` / `PopCallback` 的 public error 参数已改为 `RocketMQError`，默认回调不再接收 boxed `dyn Error`。
- pull/pop 的 broker response code 判断已直接匹配 `RocketMQError::BrokerOperationFailed`，移除了 runtime downcast helper。
- request-response callback API 已改为 `Option<&RocketMQError>`，`RequestResponseFuture` 的 cause 直接存储 `Arc<RocketMQError>`。
- request future timeout scan 与 async request 失败路径都写入 typed `RocketMQError` cause。
- `typed_error_client_flow_tests` 和 `scripts/error_architecture_guard.py` 已覆盖 callback typed boundary，阻止回退到 dyn/downcast。

**追踪**：Issue [#7945](https://github.com/mxsm/rocketmq-rust/issues/7945)，PR [#7946](https://github.com/mxsm/rocketmq-rust/pull/7946)。

**开发 checklist**：

- [x] 修改 pull/pop callback 内部错误类型为 `RocketMQError`。
- [x] 移除 `broker_response_code(error: &(dyn Error + Send + 'static))` 这类 downcast helper。
- [x] 对 broker response code 使用 typed variant 或 `ErrorKind`/spec/recovery policy。
- [x] 更新 public callback trait；本轮重构允许 breaking change，不保留旧 ABI。
- [x] 更新调用方和测试，确保回调错误判断不依赖 runtime downcast。

**验收 checklist**：

- [x] `pull_callback.rs`、`pop_callback.rs` 无 `dyn Error` downcast。
- [x] callback 错误路径可直接读取 `RocketMQError::kind()` / `spec()`。
- [x] 订阅落后、flow control 等 broker response 行为仍保持。

**建议验证**：

```powershell
cargo test -p rocketmq-client-rust pull_callback
cargo test -p rocketmq-client-rust pop_callback
cargo test -p rocketmq-client-rust typed_error_client_flow_tests
```

### T07. 收敛 client retry/fault 策略

**范围**：`rocketmq-client/src/common/retry_decision.rs`、producer send retry、route refresh、broker switch 相关代码。

**当前缺口**：

- `ClientRetryDecision::from_error` 已读取 `error.kind().spec().recovery.retry`。
- `BrokerOperationFailed { code }` 仍用 `retry_response_codes` allowlist 判断，这是 RocketMQ broker response 兼容路径，需要明确边界。
- producer/consumer 中仍有局部 retry 判断和状态处理。

**完成状态**：

- `ClientRetryDecision` 已补齐 `ClientRetryEffect`，将 retry、route refresh、switch broker、refresh leader、backoff 行为统一表达在 `retry_decision` 模块。
- producer send 失败路径已改为先读取 `producer_send_retry_decision`，再由集中 `producer_send_fault_decision` 更新 fault strategy。
- send 语境优先级已固定为：terminal send error 优先 `NoRetry`，`BrokerOperationFailed` 使用 Java 兼容 response code allowlist，其它错误回落到 `ErrorSpec.recovery.retry`。
- `retry_decision` 单元测试覆盖 `RefreshRoute`、`SwitchBroker`、`RefreshLeader`、`AfterBackoff`、`Never`、broker response allowlist 和不读取 display text。
- `scripts/error_architecture_guard.py` 已增加 client retry boundary 检查，防止 retry 决策回退到文本解析、runtime downcast 或绕开集中模块。

**追踪**：Issue [#7947](https://github.com/mxsm/rocketmq-rust/issues/7947)，PR [#7948](https://github.com/mxsm/rocketmq-rust/pull/7948)。

**开发 checklist**：

- [x] 明确 `RetryClass` 与 broker response code allowlist 的优先级。
- [x] 将 route refresh、switch broker、refresh leader、backoff 行为集中在 retry decision 模块。
- [x] 避免 producer/consumer 新增文本或局部 variant 猜测。
- [x] 对 `RetryClass::RefreshRoute/SwitchBroker/RefreshLeader/AfterBackoff/Never` 增加行为测试。
- [x] 对 broker response allowlist 写明 Java compatibility 理由。

**验收 checklist**：

- [x] client retry 不读取 display string。
- [x] 新增 `ErrorKind` 的 retry 行为来自 `ErrorSpec.recovery`。
- [x] broker response allowlist 有测试覆盖。

**建议验证**：

```powershell
cargo test -p rocketmq-client-rust retry_decision
cargo test -p rocketmq-client-rust default_mq_producer_impl
```

### T08. store/controller/auth/broker source 链保留

**范围**：`rocketmq-store`、`rocketmq-controller`、`rocketmq-auth`、`rocketmq-broker`。

**当前缺口**：

- 多处 `error.to_string()` 被用作新错误的 reason，可能丢失 source 链。
- store RocksDB、HA、mapped file、local file consume queue 等路径仍有字符串化错误。
- controller openraft/log store/state machine 会将 error 转成 `std::io::Error::other(error.to_string())`。
- auth provider/factory/loader/authorization builder 等路径仍把 error 压成字符串。
- broker processor 和 transaction queue 中仍有 `RocketMQError::Internal("...")`。

**完成状态**：

- `StoreError::RocksDb` 已改为保留 `RocketMQError` source，RocksDB message store 初始化和查询映射不再把 `RocketMQError` 压成字符串。
- `MappedFileError::MmapFailed` / `FlushFailed` 已改为保留 `std::io::Error` source，mapped buffer flush 路径不再 `to_string()`。
- broker 中可分类的 `Internal(String)` 已收敛为 `ServiceError`、`BrokerOperationFailed(SystemBusy)` 或 `response_process_failed`。
- `scripts/error_architecture_guard.py` 已增加 `SOURCE_STRINGIFICATION_ALLOWLIST`，对 store/controller/auth/broker 中仍需字符串化的 trait、runtime、protocol、auth boundary 标明原因；未登记路径会失败。
- store focused tests 覆盖 RocksDB source 和 mapped file source；guard 覆盖 controller/auth/broker 的 source-preservation allowlist。

**追踪**：Issue [#7949](https://github.com/mxsm/rocketmq-rust/issues/7949)，PR [#7950](https://github.com/mxsm/rocketmq-rust/pull/7950)。

**开发 checklist**：

- [x] 建立 source-preservation allowlist，标出必须保留 source 的类型。
- [x] 为 RocksDB、mapped file 增加 typed source wrapper；HA、serde、I/O、openraft、runtime join/semaphore 等登记 source-preservation allowlist。
- [x] store 中 `StoreError::RocksDb(String)` 等路径替换为 typed source 或 structured context。
- [x] auth 中配置、ACL loader、metadata provider、authorization builder 保留 source 或分类成 typed auth/config/storage error。
- [x] controller 中 openraft 必须转 `std::io::Error` 的地方记录边界原因，其他路径保留 typed source。
- [x] broker `Internal(String)` 场景改成 `NotInitialized`、`Storage*`、`BrokerOperationFailed` 等 typed variant。
- [x] 为每个 domain crate 增加 focused regression tests。

**验收 checklist**：

- [x] 无未登记 source 字符串化路径。
- [x] I/O、serde、storage、raft、runtime 错误能通过 source 链追因，或有明确 allowlist。
- [x] `Internal(String)` 只剩无法分类且有 allowlist 的场景。

**建议验证**：

```powershell
cargo test -p rocketmq-store
cargo test -p rocketmq-controller
cargo test -p rocketmq-auth
cargo test -p rocketmq-broker
rg -n "error\\.to_string\\(\\)|RocketMQError::Internal|StoreError::RocksDb\\(" rocketmq-store rocketmq-controller rocketmq-auth rocketmq-broker --glob "*.rs"
```

### T09. 全仓库 redaction 收口

**范围**：`rocketmq-error`、`rocketmq-common`、`rocketmq-client`、`rocketmq-auth`、dashboard backend、logging/metrics 输出。

**当前缺口**：

- `Sensitive<T>` 已存在，但不少 auth/client/common 类型仍使用本地 redaction helper。
- `plain_access_resource` 等 auth migration 类型仍需核对 signature/token/password 字段是否全部脱敏。
- guard 当前主要覆盖 error/common/client/remoting 的敏感 Debug 字段，不覆盖 dashboard/tools 全部输出。

**完成状态**：

- `scripts/error_architecture_guard.py` 已定义敏感字段关键词表，并扩展到 `rocketmq-auth`、dashboard web backend、admin tools、remoting/client/common 的关键 Debug 输出。
- guard 已增加 `#[derive(Debug)]` 结构体敏感字段检查，防止 secret/password/token/signature/credential 字段绕过手写 redaction。
- `UserInfo`、`BrokerConfig`、TLS key password、client ACL hook、auth context/resource、admin tools user request、dashboard auth/ACL/config 模型均已增加 redacted Debug/Display 或 no-secret regression tests。
- dashboard ACL user list API 不再把 broker 返回的 password 映射到 HTTP response。

**追踪**：Issue [#7951](https://github.com/mxsm/rocketmq-rust/issues/7951)，PR [#7952](https://github.com/mxsm/rocketmq-rust/pull/7952)。

**开发 checklist**：

- [x] 定义敏感字段关键词表：secret、password、token、signature、authorization、credential。
- [x] 将可复用模型迁移到 `Sensitive<T>` 或统一 helper。
- [x] 对 `Display`、`Debug`、tracing structured fields、API response、CLI output 分别建立 redaction 测试。
- [x] 修复 auth migration/context 中所有未脱敏 signature/token/password Debug 字段。
- [x] 扩展 guard 到 auth、dashboard backend、tools 的关键输出类型。
- [x] 确认测试 fixture 中明文 secret 只存在于测试输入，不进入输出断言。

**验收 checklist**：

- [x] `SessionCredentials`、auth config、user/password、plain access config/resource 都有 no-secret tests。
- [x] guard 能阻止新增敏感 Debug field。
- [x] API/CLI/log 输出默认不含敏感原文。

**建议验证**：

```powershell
cargo test -p rocketmq-error error_context_redaction
cargo test -p rocketmq-client-rust session_credentials
cargo test -p rocketmq-auth redacts
py scripts\error_architecture_guard.py
```

### T10. 扩展并接入 CI guard

**范围**：`scripts/error_architecture_guard.py`、`.github/workflows/rocketmq-rust-ci.yaml`、dashboard CI workflows。

**当前缺口**：

- `scripts/error_architecture_guard.py` 本地可通过。
- 当前未在 `.github/workflows` 中看到调用。
- guard 只覆盖部分范围：error/common public surface、processor unsupported-code、remoting/proxy adapter token、部分 redaction。

**完成状态**：

- root CI 已在 format check 后、clippy 前运行 `python3 scripts/error_architecture_guard.py`。
- dashboard web backend CI 已运行同一个 guard，并将 `scripts/error_architecture_guard.py` 纳入 workflow 触发路径。
- `docs/error-architecture-guard.md` 已说明本地运行方式、CI 入口、覆盖范围和 allowlist 规则。

**追踪**：Issue [#7953](https://github.com/mxsm/rocketmq-rust/issues/7953)，PR [#7954](https://github.com/mxsm/rocketmq-rust/pull/7954)。

**开发 checklist**：

- [x] 将 `py scripts\error_architecture_guard.py` 接入 root CI。
- [x] 如果 dashboard shared/backend 被纳入 guard，dashboard web CI 也调用对应检查。
- [x] 扩展 guard：library/shared public `anyhow` allowlist。
- [x] 扩展 guard：unmapped `ErrorKind`、missing `ErrorSpec`、missing protocol/recovery/observe/redaction。
- [x] 扩展 guard：processor 通用 `SystemError` / `InvalidParameter` 新增检查。
- [x] 扩展 guard：source stringification allowlist。
- [x] 扩展 guard：sensitive output allowlist。
- [x] 增加 guard 文档，说明如何新增 allowlist 以及何时不允许新增。

**验收 checklist**：

- [x] CI 中能看到 error architecture guard step。
- [x] guard 失败会输出 `path:line`。
- [x] guard 不依赖本机 `python` alias；Windows 上使用 `py` 或 CI 指定 Python。

**建议验证**：

```powershell
py scripts\error_architecture_guard.py
rg -n "error_architecture_guard" .github scripts
```

### T11. CLI/tools 错误出口接入 `CliSpec`

**范围**：`rocketmq-tools/rocketmq-admin-*`、`rocketmq-tools/rocketmq-store-inspect`。

**当前缺口**：

- `rocketmq-tools` 已通过 `CliErrorView`、`CliExitCode`、admin-core `error_code` 字段接入中心 spec。
- TUI 的 `anyhow::Result` 保留在 terminal runtime/app boundary；admin-core reusable command executor 继续返回 `RocketMQResult`。
- CLI exit 已从 `ErrorSpec.cli` 派生；store-inspect one-shot binary 也通过同一 adapter 输出和退出。

**完成状态**：

- `rocketmq-error::CliErrorView` 已成为统一 `RocketMQError -> exit code/category/message/context` CLI adapter，读取 `spec().cli`、stable code、category 和 redacted context。
- `rocketmq-admin-cli` outer boundary 已返回 `CliSpec` 派生 exit code；缺少命令、unsupported completion shell 和 command executor typed error 都走 `CliErrorView`。
- `rocketmq-admin-core` 批量失败 view model 新增 `error_code`，错误消息使用 stable public message + redacted context，保留原 `error` 字段兼容现有展示。
- `rocketmq-store-inspect` 已从 panic/`unwrap()` 文件读取边界改为 `RocketMQResult<()>`，binary 使用同一 `CliErrorView`。
- `scripts/error_architecture_guard.py` 已把 CLI adapter、admin-core error view 和 store-inspect CLI boundary 加入 required mapping adapters 检查。

**追踪**：Issue [#7955](https://github.com/mxsm/rocketmq-rust/issues/7955)，PR [#7956](https://github.com/mxsm/rocketmq-rust/pull/7956)。

**开发 checklist**：

- [x] 在 CLI outer boundary 保留 `anyhow`，内部 command executor 返回 typed error 或 typed view model。
- [x] 新增 `RocketMQError -> exit code/category/message` adapter，读取 `spec().cli`。
- [x] command failure view model 使用 stable code，而不是只存 `error.to_string()`。
- [x] 对 admin-core 中可复用逻辑避免 `anyhow` 泄漏。
- [x] 对 `rocketmq-store-inspect` 这类 one-shot tool 明确 allowlist 或 typed boundary。
- [x] 增加 CLI exit code tests。

**验收 checklist**：

- [x] CLI 对 typed error 输出稳定 exit category。
- [x] 可复用工具库不暴露未登记 `anyhow::Result`。
- [x] 错误展示不泄露 secret/token/signature。

**建议验证**：

```powershell
cargo test -p rocketmq-admin-cli
cargo test -p rocketmq-admin-tui
cargo test -p rocketmq-admin-core
cargo test -p rocketmq-store-inspect
py scripts\error_architecture_guard.py
rg -n "anyhow::Result|spec\\(\\)\\.cli|CliExitCode" rocketmq-tools --glob "*.rs"
```

### T12. standalone project 对齐和验证

**范围**：`rocketmq-example`、dashboard GPUI、dashboard Tauri、dashboard web backend/common。

**当前缺口**：

- standalone 项目不由 root workspace clippy 覆盖，需要按各自目录独立验证。
- dashboard common/web/tauri/gpui 中的 `anyhow::Result` 已由 guard allowlist 限定在 build script、process/app boundary 或已登记 dashboard alignment 边界。
- dashboard web backend HTTP boundary 已接入 `spec().http`，并由 error architecture guard 覆盖。

**完成状态**：

- `rocketmq-example` standalone `cargo fmt --all` 与 `cargo clippy --all-targets -- -D warnings` 已通过。
- dashboard GPUI standalone `cargo fmt --all` 与 `cargo clippy --all-targets --all-features -- -D warnings` 已通过。
- dashboard Tauri `src-tauri` standalone `cargo fmt --all` 与 `cargo clippy --all-targets --all-features -- -D warnings` 已通过。
- dashboard web backend standalone `cargo fmt --all`、`cargo clippy --all-targets --all-features -- -D warnings` 与 `cargo build --all-targets --all-features` 已通过。
- root workspace `cargo fmt --all`、`cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` 与 `py scripts\error_architecture_guard.py` 已通过。

**追踪**：Issue [#7957](https://github.com/mxsm/rocketmq-rust/issues/7957)，PR [#7958](https://github.com/mxsm/rocketmq-rust/pull/7958)。

**开发 checklist**：

- [x] 根据共享 API 变更更新 `rocketmq-example`。
- [x] 根据 dashboard common typed result 变更更新 GPUI/Tauri/Web。
- [x] Tauri backend 中 `anyhow` 留在 app boundary，服务层按 typed error 收口。
- [x] Web backend 完成 HTTP adapter 后同步 common provider。
- [x] 分别运行各 standalone 目录的 clippy/build。

**验收 checklist**：

- [x] root workspace validation 通过。
- [x] `rocketmq-example` standalone clippy 通过。
- [x] dashboard GPUI standalone clippy 通过。
- [x] dashboard Tauri `src-tauri` standalone clippy 通过。
- [x] dashboard web backend clippy/build 通过。

**建议验证**：

```powershell
cd rocketmq-example
cargo fmt --all
cargo clippy --all-targets -- -D warnings

cd ..\rocketmq-dashboard\rocketmq-dashboard-gpui
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings

cd ..\rocketmq-dashboard-tauri\src-tauri
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings

cd ..\..\rocketmq-dashboard-web\backend
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
```

## 推荐执行顺序

1. T01：先补齐 `rocketmq-error` 合约字段和测试。
2. T10：让现有 guard 进入 CI，先阻止 regression。
3. T02：清理 public `anyhow`，同时建立 allowlist。
4. T03：迁移 remoting/broker/namesrv 通用错误出口。
5. T06 + T07：完成 client callback typed 化和 retry/fault 策略集中。
6. T04：收敛 proxy gRPC mapper。
7. T05：接入 dashboard HTTP mapper。
8. T08：逐 crate 清理 source 字符串化。
9. T09：全仓库 redaction guard 扩展。
10. T11 + T12：tools/standalone 收口和最终验证。

## 总体验收 Checklist

- [x] 全仓库无 `RocketmqError`、`LegacyRocketMQResult`、`LegacyResult`。
- [x] `rocketmq-error` 无 public `anyhow` alias。
- [x] library/shared API 无未登记 `anyhow::Result`。
- [x] 每个 `ErrorKind` 有唯一 `ErrorSpec`。
- [x] `ErrorSpec` 包含 code、scope、category、public message、remoting、grpc、http、cli、recovery、observe、redaction。
- [x] remoting/gRPC/HTTP/CLI 外部出口读取中心 spec 或有明确 local-only allowlist。
- [x] broker/namesrv processor 通用错误无未登记手写 response code；Java-compatible 本地 code 有 guard allowlist。
- [x] client callback 内部事实源不再是 `dyn Error` downcast。
- [x] client retry/fault 行为来自 `RetryClass` 和明确 broker response allowlist。
- [x] store/controller/auth/broker 不再无登记地把 source `to_string()` 后塞入新错误。
- [x] secret/token/signature/password 默认 redacted。
- [x] `scripts/error_architecture_guard.py` 进入 CI。
- [x] root workspace 通过 fmt/clippy。
- [x] 受影响 standalone 项目分别通过各自 clippy/build。

**收口追踪**：Issue [#7959](https://github.com/mxsm/rocketmq-rust/issues/7959)，PR [#7960](https://github.com/mxsm/rocketmq-rust/pull/7960)。

## 最终验证矩阵

```powershell
# Root workspace
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings

# Error architecture guard
py scripts\error_architecture_guard.py

# Focused root workspace tests
cargo test -p rocketmq-error
cargo test -p rocketmq-remoting
cargo test -p rocketmq-client-rust
cargo test -p rocketmq-broker
cargo test -p rocketmq-namesrv
cargo test -p rocketmq-store
cargo test -p rocketmq-controller
cargo test -p rocketmq-auth
cargo test -p rocketmq-proxy

# Static inspection gates
rg -n "RocketmqError|LegacyRocketMQResult|LegacyResult" --glob "*.rs"
rg -n "anyhow::Result" --glob "*.rs"
rg -n "RocketMQError::Internal|error\\.to_string\\(\\)|map_err\\([^\\n]*to_string" --glob "*.rs"
rg -n "secretKey|SecurityToken|signature|password|token" rocketmq-client rocketmq-auth rocketmq-dashboard --glob "*.rs"
```

## 维护要求

- 每完成一个任务，在本文件对应 checklist 勾选，并补充 PR 或 commit 链接。
- 如果某项无法清零，必须新增 allowlist，说明原因、责任 owner 和后续复查时间。
- 每次修改 error kernel、boundary adapter、redaction guard 后，都要更新本文件和 contribution guide。
