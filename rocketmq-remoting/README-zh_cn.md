# rocketmq-remoting

[![Crates.io](https://img.shields.io/crates/v/rocketmq-remoting.svg)](https://crates.io/crates/rocketmq-remoting)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE-APACHE)

`rocketmq-remoting` 是
[rocketmq-rust](https://github.com/mxsm/rocketmq-rust) 工作空间使用的网络协议、
编解码、客户端传输和服务端运行时基础。它建模 RocketMQ remoting command、
与 Java 兼容的 request/response code、header、body、路由元数据、心跳载荷、
RPC hook、Tokio 连接、连接池、TLS 传输和请求处理器分发。

该 crate 是 `rocketmq-client-rust`、`rocketmq-broker`、`rocketmq-namesrv`、
`rocketmq-proxy` 以及 controller 协议集成共享的 remoting 层。

[English](README.md)

## 架构

![rocketmq-remoting architecture](../resources/remoting-architecture.svg)

该 crate 主要分为五层：

- **协议模型**：`RemotingCommand`、`RequestCode`、response code、command
  header、command body、route data、heartbeat data、topic metadata、
  subscription metadata、broker metadata、controller metadata 和 auth 相关
  协议结构。
- **编解码与传输**：Tokio `Encoder`/`Decoder` 实现、framed command IO、
  `Connection`、`Channel`、TLS accept/connect helper 和自适应 encode buffer。
- **客户端运行时**：`RocketmqDefaultClient`、NameServer 地址选择、连接复用、
  基于 `opaque` 的请求复用、重连退避、熔断器和可选连接池指标。
- **服务端运行时**：TCP accept loop、单连接任务、idle 检测、优雅关闭、
  channel event 和 `RequestProcessor` 分发。
- **扩展点**：RPC hook、`RequestProcessorV2` core/plugin 分发、observability
  计数器、SIMD JSON 支持以及公开 re-export/prelude。

## 能力概览

- 通过 `RemotingCommandCodec` 编解码 RocketMQ remoting frame。
- 定义与 Java 兼容的 request/response code，覆盖 broker、NameServer、
  controller、lite subscription、POP、auth、cold-data 等 request 范围。
- 提供常见 RocketMQ header 和 body 的协议模型。
- 提供异步 remoting client，支持 NameServer 列表管理、request/response
  关联、one-way 调用、后台扫描、RPC hook 和优雅关闭。
- 可选增强 `ConnectionPool`，支持指标、idle 清理、利用率统计和健康度跟踪。
- 提供 Tokio remoting server，管理连接生命周期和请求处理器。
- 默认启用 TLS 支持，并在运行时支持 disabled、permissive、enforcing server
  mode。
- 提供基于 GAT 的 `RequestProcessorV2`，用于零分配 core dispatch，并支持
  动态 plugin processor。
- 可通过 `simd` feature 启用 SIMD JSON 解析。
- 可通过 `observability` feature 输出 OpenTelemetry 指标。

## 协议兼容

协议兼容测试保证 Rust 模型与 Apache RocketMQ Java remoting 协议保持一致：

- 169 个 Java request code 被断言为协议已定义。
- 63 个 Java response code 会执行 round trip。
- 保留 Rust 历史 ACL request-code alias。
- 未知 request code 保持为 `RequestCode::Unknown`。
- Request/response command 的 codec round trip 会保留 wire fields、
  `opaque`、flags、remarks、headers 和 bodies。

可执行兼容基线见
[`tests/protocol_compatibility_tests.rs`](tests/protocol_compatibility_tests.rs)。

## 环境要求

- 使用仓库 Rust toolchain。当前 workspace 使用 `nightly`，因为该 crate 的
  `RequestProcessorV2` 使用了 `impl_trait_in_assoc_type`。
- 异步 client 和 server 使用需要 Tokio runtime。
- 启用严格 TLS server mode 时需要对应 TLS 证书材料。

## 安装

在 workspace 根目录执行：

```bash
cargo build -p rocketmq-remoting
```

作为 workspace dependency：

```toml
[dependencies]
rocketmq-remoting = { path = "../rocketmq-remoting" }
```

启用可选 feature：

```toml
[dependencies]
rocketmq-remoting = { path = "../rocketmq-remoting", features = ["simd", "observability"] }
```

## 快速开始

### 编解码 Command

```rust
use bytes::{Bytes, BytesMut};
use rocketmq_remoting::codec::remoting_command_codec::RemotingCommandCodec;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::RequestCode;
use tokio_util::codec::{Decoder, Encoder};

fn main() -> rocketmq_error::RocketMQResult<()> {
    let command = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new("TopicA", Some(true)),
    )
    .set_body(Bytes::from_static(b"payload"));

    let mut codec = RemotingCommandCodec::new();
    let mut buffer = BytesMut::new();

    codec.encode(command, &mut buffer)?;
    let decoded = codec.decode(&mut buffer)?.expect("complete frame");

    assert_eq!(decoded.request_code(), RequestCode::GetRouteinfoByTopic);
    assert_eq!(decoded.body().map(|body| body.as_ref()), Some(&b"payload"[..]));

    Ok(())
}
```

### 使用 Prelude

```rust
use rocketmq_remoting::prelude::*;

let header = PullMessageRequestHeader::default();
let command = RemotingCommand::create_request_command(RequestCode::PullMessage, header);

assert_eq!(command.request_code(), RequestCode::PullMessage);
```

### 创建 Client Runtime

```rust
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::clients::RemotingClient;

# async fn example() {
let client = RocketmqDefaultClient::new(
    Arc::new(TokioClientConfig::default()),
    DefaultRemotingRequestProcessor,
);

client
    .update_name_server_address_list(vec![CheetahString::from("127.0.0.1:9876")])
    .await;
# }
```

### 运行连接池示例

```bash
cargo run -p rocketmq-remoting --example connection_pool_usage
```

## 核心 API

| 领域 | 重要类型 |
| --- | --- |
| Protocol command | `RemotingCommand`, `RemotingCommandType`, `SerializeType`, `LanguageCode` |
| Codes | `RequestCode`, `ResponseCode`, `RemotingSysResponseCode` |
| Codec | `RemotingCommandCodec`, `CompositeCodec`, `EncodeBuffer` |
| Client | `RemotingClient`, `RocketmqDefaultClient`, `TokioClientConfig`, `ConnectionPool` |
| Server | `rocketmq_tokio_server::run`, `RocketMQServer`, `ConnectionHandler` |
| Runtime hooks | `RemotingService`, `RPCHook`, `InvokeCallback` |
| Processing | `RequestProcessor`, `RequestProcessorV2`, `ProcessorDispatcher`, `PluginProcessorRegistry` |
| Protocol models | headers、bodies、route data、static topic mapping、heartbeat、subscriptions、broker metadata |
| TLS | `TlsConfig`, `TlsMode`, `TlsServerRuntime`, TLS connect/accept helpers |

## Feature Flags

| Feature | 默认启用 | 说明 |
| --- | --- | --- |
| `tls` | 是 | 启用 TLS 传输依赖和运行时 TLS helper。 |
| `simd` | 否 | 启用 `simd-json`，用于加速 JSON 解析路径。 |
| `observability` | 否 | 通过 `rocketmq-observability` 输出 remoting 指标。 |

该 crate 默认 feature set 为 `["tls"]`。

## Crate 结构

```text
rocketmq-remoting/
  src/lib.rs                         public modules 与顶层 re-export
  src/protocol/                      command model、headers、bodies、route、heartbeat、admin data
  src/code/                          request、broker request、response code 定义
  src/codec/                         Tokio codec 实现
  src/connection.rs                  framed remoting connection
  src/net/                           channel 抽象
  src/clients/                       async clients、connection pool、reconnect、NameServer selector
  src/remoting_server/               Tokio remoting server runtime
  src/runtime/                       config、request processors、hooks、connection context
  src/rpc/                           RPC request/response helper 与 RPC client 实现
  src/tls.rs                         TLS connect/accept runtime
  src/smart_encode_buffer.rs         自适应 encode buffer
  examples/                          可运行示例与快速性能探针
  benches/                           codec、network、client、SIMD benchmarks
  tests/                             re-export、processor v2、protocol compatibility tests
```

## 验证

常用聚焦检查：

```bash
cargo test -p rocketmq-remoting --lib
cargo test -p rocketmq-remoting --test protocol_compatibility_tests --test processor_v2_tests
cargo test -p rocketmq-remoting --test test_reexports --test test_enhanced_reexports --test test_top_level_reexports
cargo clippy -p rocketmq-remoting --all-targets --all-features -- -D warnings
```

如果修改仓库范围 Rust 代码，需要在根目录执行 workspace 验证：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Codec 和传输敏感路径提供 benchmark：

```bash
cargo bench -p rocketmq-remoting
```

## License

基于 [Apache License, Version 2.0](../LICENSE-APACHE) 许可发布。
