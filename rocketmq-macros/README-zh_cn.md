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
| `RequestHeaderCodecV2` | 主用 | 为具名 Rust struct 生成 `CommandCustomHeader::to_map`、低分配 `encode_into_map` 和借用式 `FromMap::from`，支持确定性 alias、required、泛型和 flattened nested header。 |
| `RequestHeaderCodec` | 兼容 | 早期 request-header codec derive，支持 Java 风格 camelCase key、`#[required]`、可选字段、primitive parse 和 flattened nested header。 |
| `RemotingSerializable` | 工具 | 为类型实现 `crate::protocol::RemotingSerializable`。当前多数 remoting 路径优先使用 serde-backed blanket impl。 |

## 工作方式

```text
typed protocol header struct
        |
        | #[derive(RequestHeaderCodecV2)]
        v
generated constants + CommandCustomHeader::encode_into_map()
        |
        v
RocketMQ remoting ext fields: HashMap<CheetahString, CheetahString>
        |
        v
generated FromMap::from() with required-field validation and parsing
```

生成代码面向 `rocketmq-protocol` 的公共 contract：

- `rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader`
- `rocketmq_protocol::protocol::command_custom_header::FromMap`
- `rocketmq_protocol::HeaderMap`

derive 能自动解析 `Cargo.toml` 中重命名后的 `rocketmq-protocol` 依赖。生成代码或 re-export 场景也可以显式指定路径：

```rust
#[derive(RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "path::to::protocol_api")]
struct Header {
    queue_id: i32,
}
```

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
- `CommandCustomHeader::encode_into_map`，将 flattened 字段直接写入同一个目标 map；
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
rename 指定的 wire key。`serde(alias = "...")` 按声明顺序在 canonical key 之后查找，因此结果不受
`HashMap` 迭代顺序影响。

tuple/unit struct、enum、union、空或冲突 wire key、`#[required] Option<T>`、required 与
`serde(default)` 组合、scalar 与 `serde(flatten)` 组合都会在编译期报错。跨 flattened child 的 key
冲突由仓库 schema comparator 检查，因为 derive macro 无法检查另一类型的字段。

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
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | 旧版 `RequestHeaderCodec` 展开逻辑。 |
| [`src/request_header_codec_v2/`](src/request_header_codec_v2/) | V2 属性解析、语义模型、校验和代码生成。 |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | `RemotingSerializable` derive 展开逻辑。 |
| [`Cargo.toml`](Cargo.toml) | Proc-macro crate 配置和宏解析依赖。 |

## 环境要求

- Stable Rust `1.95.0`，使用仓库固定的工具链。
- 使用仓库中的 [`../rust-toolchain.toml`](../rust-toolchain.toml) 工具链。
- 直接或重命名的 `rocketmq-protocol` 依赖，或显式 `request_header_codec_v2(crate = "...")` 路径。

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

依赖重命名无需修改源代码。`tests/fixtures/renamed-consumer` 会通过离线 Cargo check 验证该契约。

## 验证

该 crate 的聚焦检查：

```bash
cargo test -p rocketmq-macros --lib
cargo test -p rocketmq-protocol --test request_header_codec_v2_wire_snapshot
cargo test -p rocketmq-protocol --test request_header_codec_v2_ui
cargo check --offline --manifest-path rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.toml
```

由于该宏被大多数 protocol header 使用，修改展开逻辑后还应验证 protocol crate：

```bash
cargo test -p rocketmq-protocol --lib
```

如果修改 Rust 代码，需要在仓库根目录执行 workspace 级验证：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## License

RocketMQ-Rust 使用 Apache License 2.0。详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
