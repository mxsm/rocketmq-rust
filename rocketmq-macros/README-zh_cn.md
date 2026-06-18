# rocketmq-macros

[English](README.md) | [简体中文](README-zh_cn.md)

RocketMQ-Rust protocol 类型、请求头和 remoting 序列化辅助能力使用的 procedural macros。

`rocketmq-macros` 是 RocketMQ-Rust workspace 内部使用的小型 proc-macro crate，用于消除 request/response header
定义中的重复 protocol glue。它的核心职责是为 RocketMQ remoting header 生成 `CommandCustomHeader` 和 `FromMap`
实现，在保持 Java 兼容 wire key 的同时，让 Rust 结构体保持类型化和可维护。

该 crate 是基础设施，不是运行时组件。大多数应用代码应使用更高层的 `rocketmq-remoting`、`rocketmq-client-rust`、
`rocketmq-broker` 或服务 crate，而不是直接依赖这些宏。

## 能力边界

| 宏 | 状态 | 生成能力 |
|----|------|----------|
| `RequestHeaderCodecV2` | 主用 | 为具名 Rust struct 生成 `CommandCustomHeader::to_map` 和 `FromMap::from`，支持优化后的 map 解码、required 字段检查、`serde(rename)`、`serde(alias)` 和 flattened nested header。 |
| `RequestHeaderCodec` | 兼容 | 早期 request-header codec derive，支持 Java 风格 camelCase key、`#[required]`、可选字段、primitive parse 和 flattened nested header。 |
| `RemotingSerializable` | 工具 | 为类型实现 `crate::protocol::RemotingSerializable`。当前多数 remoting 路径优先使用 serde-backed blanket impl。 |

## 工作方式

```text
typed protocol header struct
        |
        | #[derive(RequestHeaderCodecV2)]
        v
generated constants + CommandCustomHeader::to_map()
        |
        v
RocketMQ remoting ext fields: HashMap<CheetahString, CheetahString>
        |
        v
generated FromMap::from() with required-field validation and parsing
```

生成代码有意面向 `rocketmq-remoting` 使用的 protocol contract：

- `crate::protocol::command_custom_header::CommandCustomHeader`
- `crate::protocol::command_custom_header::FromMap`
- `crate::protocol::RemotingSerializable`
- `cheetah_string::CheetahString`
- `rocketmq_error::RocketMQError`

由于宏展开使用 `crate::protocol...` 路径，这些宏主要面向暴露兼容 protocol 模块的 crate。在当前 workspace 中，
主要使用方是 `rocketmq-remoting`。

## 快速开始

在表示 RocketMQ remoting header 的具名 struct 上使用 `RequestHeaderCodecV2`：

```rust
use rocketmq_macros::RequestHeaderCodecV2;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequestHeader {
    #[required]
    pub producer_group: cheetah_string::CheetahString,
    #[required]
    pub topic: cheetah_string::CheetahString,
    pub queue_id: Option<i32>,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub batch: Option<bool>,
}
```

该 derive 会生成：

- wire key 对应的 associated string constants；
- `CommandCustomHeader::to_map`，并省略 `None` 值；
- `FromMap::from`，将 `CheetahString` map value 转回类型化字段；
- `#[required]` 字段的缺失校验；
- 非 required、非 `Option` 字段缺失时使用默认值。

## 字段映射

| Rust 字段形态 | 序列化行为 | 反序列化行为 |
|---------------|------------|--------------|
| `CheetahString` | 直接写入 ext-field map。 | 直接读取，非 required 缺失时使用默认值。 |
| `String` | 转换为 `CheetahString`。 | 转回 `String`。 |
| Primitive 类型 | 使用 `to_string()` 转换。 | 使用 `FromStr` 解析；required 字段解析失败时返回 header 错误。 |
| `Option<T>` | 仅在 `Some` 时写入。 | 缺失时为 `None`；存在的 primitive value 会被解析。 |
| `#[serde(flatten)]` nested header | 合并 nested header map。 | 通过 nested type 的 `FromMap::from` 重建。 |

`RequestHeaderCodecV2` 默认将 snake_case 字段名转换为 camelCase wire key；如果存在 `serde(rename = "...")`，则使用
rename 指定的 wire key。`serde(alias = "...")` 会在解码时作为兼容 wire name 接受。

## Required 字段

`#[required]` 表示 `FromMap::from` 时必须存在该 header 字段：

```rust
#[derive(RequestHeaderCodecV2)]
pub struct QueryMessageRequestHeader {
    #[required]
    pub topic: cheetah_string::CheetahString,
    pub key: Option<cheetah_string::CheetahString>,
}
```

这对应 Java RocketMQ 中 `@CFNotNull` 的意图。缺失 required 字段会返回
`RocketMQError::DeserializeHeaderError`，错误信息使用生成后的 wire-key name。

## Crate 结构

| 路径 | 职责 |
|------|------|
| [`src/lib.rs`](src/lib.rs) | 公共 proc-macro 入口和共享类型辅助函数。 |
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | `RequestHeaderCodec` 和 `RequestHeaderCodecV2` 展开逻辑。 |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | `RemotingSerializable` derive 展开逻辑。 |
| [`Cargo.toml`](Cargo.toml) | Proc-macro crate 配置和宏解析依赖。 |

## 环境要求

- Rust `1.85.0` 或更新版本。
- 使用仓库中的 [`../rust-toolchain.toml`](../rust-toolchain.toml) 工具链。
- 消费方 crate 需要为生成的 protocol trait 实现暴露兼容的 `crate::protocol` 模块路径。

## 安装

在当前 workspace 内使用：

```toml
[dependencies]
rocketmq-macros = { path = "../rocketmq-macros" }
```

外部项目使用：

```toml
[dependencies]
rocketmq-macros = "1.0.0"
```

外部使用只有在消费方 crate 暴露生成代码所需的相同 protocol traits 和辅助类型时才实际可行。

## 验证

该 crate 的聚焦检查：

```bash
cargo test -p rocketmq-macros --lib
```

由于这些宏被 `rocketmq-remoting` 大量使用，修改宏展开逻辑后还应验证下游 protocol header：

```bash
cargo test -p rocketmq-remoting --lib
```

如果修改 Rust 代码，需要在仓库根目录执行 workspace 级验证：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## License

RocketMQ-Rust 使用 Apache License 2.0。详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
