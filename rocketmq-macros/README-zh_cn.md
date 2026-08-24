# rocketmq-macros

[English](README.md) | [简体中文](README-zh_cn.md)

RocketMQ-Rust 协议类型和 Remoting Header 使用的过程宏。

`rocketmq-macros` 是编译期基础设施。多数应用代码应使用上层 client、broker 或 remoting crate，不应直接依赖这些宏。

## Request Header Derive

| 宏 | 状态 | 用途 |
| --- | --- | --- |
| `RequestHeaderCodecV3` | 推荐 | 生成类型化 map/source codec、wire schema、校验、键解析、兼容适配器，以及经过审查的可选直接编码。 |
| `RequestHeaderCodecV2` | 已废弃 | 加固 V2 wire 契约的冻结兼容适配器；生产 Header 禁止新增使用。 |
| `RequestHeaderCodec` | 已废弃 | 保留最早 Request Header quirks 的冻结兼容适配器，仅用于下游源码兼容。 |
| `RemotingSerializable` | 工具 | 为类型生成 Remoting 序列化辅助实现。 |

仓库中登记的全部生产请求头和响应头都已使用 V3。所有新增 request-header 代码必须使用 V3；V1 和 V2 仅是冻结的兼容适配器。旧 derive 至少保留一个发布窗口，只会在未来的破坏性版本中删除。

## 快速开始

V3 只把专用 `#[header(...)]` 元数据作为 RocketMQ wire 契约。Serde 属性继续独立服务 JSON/DTO，不能用于推断 Header 的 key、default、alias 或 flatten。

```rust
use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV3;

#[derive(Debug, RequestHeaderCodecV3)]
#[header(
    type_id = "example::SendMessageRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader"
)]
struct SendMessageRequestHeader {
    #[header(required)]
    producer_group: CheetahString,
    #[header(required)]
    topic: CheetahString,
    #[header(default, default_semantic = "literal:0")]
    sys_flag: i32,
    batch: Option<bool>,
}
```

生成结果包括：

- 带稳定类型 ID 和字段/flatten schema 的 `HeaderCodec`；
- `CommandCustomHeader` 与 `FromMap` 兼容适配器；
- canonical key 与 decode alias 的确定性解析和冲突处理；
- 显式 required/default/validation/range 语义；
- 输入支持时的借用式、零拷贝 field-source 解码；
- 默认 `MapOnly` 编码，以及仅对显式审查过的 `fast` Header 生成直接编码。

## 元数据规则

容器属性：

| 属性 | 含义 |
| --- | --- |
| `type_id = "..."` | 必填的稳定 Rust schema 标识。 |
| `java_class = "..."` | 存在 Java 对应类型时填写 Java FQCN；Rust-only Header 不填写。 |
| `crate = "path"` | 可选的 protocol crate 路径覆盖，支持依赖重命名。 |
| `fast` | 只有完成正确性和性能审查后才启用生成式直接编码。 |
| `validate = "path"` | 编码首次写入前和解码完成后执行类型化校验。 |
| `legacy_shim = "manual"` | 已有审计过的手工兼容实现时避免生成重复实现。 |

字段属性：

| 属性 | 含义 |
| --- | --- |
| `required` | 输入缺失时报错。 |
| `default` / `default_with = "path"` | 显式定义字段缺失行为，并同时声明 `default_semantic`。 |
| `key = "..."` | canonical wire key；省略时使用 Rust 字段名。 |
| `alias = "..."` | 仅用于解码的历史 key；V3 不会输出 alias。 |
| `flatten, presence = "always|any"` | 嵌套 Header 的继承/存在性契约。 |
| `range = "i32|i64"` | 把无符号 Rust 字段限制到对应 Java 有符号域。 |

生产字段不填写 `java_type`。V3 会根据 Rust 类型推断普通 wire kind。只有当无符号 Rust 字段受 Java 有符号 `int` 或 `long` 约束时才填写 `range`；有符号 Rust 字段和 Rust-only 无符号字段都不需要填写。

## 运行路径与回退

所有登记 Header 都实现了类型化 codec 和对象安全兼容适配器，因此 V3 已是生产默认路径。普通 Header 返回 `MapOnly`，Remoting 编码器会物化 canonical extension fields，不依赖可变 `Arc` 访问。经过审查的热 Header 可以对 ROCKETMQ frame 使用 `DirectBinary`，并可提供直接 JSON fields。如果命令已经包含物化字段或动态字段，同一份类型化 schema 会负责冲突解析，map 路径仍是权威回退。

回退按 Header 和命令生效，不改变 wire 契约，也不会在每条消息上增加全局开关或环境变量查询。

## 迁移旧 Header

V2 元数据不会被静默解释成 V3。必须对照固定 Java schema 审核后显式转换：

| V2 来源 | V3 决策 |
| --- | --- |
| `#[required]` | `#[header(required)]` |
| `serde(rename = "...")` | 确认是 wire key 后改为 `#[header(key = "...")]` |
| `serde(alias = "...")` | 改为 `#[header(alias = "...")]`，必要时明确冲突策略 |
| `serde(default)` | 改为带 `default_semantic` 的 `header(default)` 或已审查的 `default_with` |
| `serde(flatten)` | 改为 `#[header(flatten, presence = "always|any")]` |
| 对应 Java `int`/`long` 的无符号字段 | `range = "i32"` / `range = "i64"` |

V2 只应用于尚未完成迁移的既有下游模型。RocketMQ-Rust 新增生产 Header 必须使用 V3，并进入 checked-in schema inventory，否则 migration guard 会拒绝。

V1（`RequestHeaderCodec`）为源码兼容而冻结，包括其历史解析和 decode quirks。不要将其用于新代码；现有 V1 Header 应直接迁移到显式的 V3 model。

## 重命名 Protocol 依赖

derive 会通过 Cargo 元数据解析 `rocketmq-protocol`。生成代码或 re-export 场景可显式覆盖：

```rust
#[derive(RequestHeaderCodecV3)]
#[header(type_id = "example::Header", crate = "protocol_api")]
struct Header {
    #[header(required)]
    queue_id: i32,
}
```

独立项目 `tests/fixtures/renamed-consumer` 同时验证 V3 路径和保留的 V2 兼容能力。

## Crate 结构

| 路径 | 用途 |
| --- | --- |
| [`src/lib.rs`](src/lib.rs) | 公开 derive 入口和共享解析辅助函数。 |
| [`src/request_header_codec_v3/`](src/request_header_codec_v3/) | canonical V3 元数据、语义模型、profile 校验和代码生成。 |
| [`src/request_header_codec_v3/legacy_v1.rs`](src/request_header_codec_v3/legacy_v1.rs) 与 [`legacy_v2.rs`](src/request_header_codec_v3/legacy_v2.rs) | 基于 canonical model 的冻结 V1/V2 语法适配器和兼容代码生成。 |
| [`src/request_header_codec_v2/`](src/request_header_codec_v2/) | 已废弃的 V2 public syntax parser 和 adapter。 |
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | 已废弃的 V1 parse/wrapper entry，转发到冻结兼容适配器。 |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | Remoting 序列化 derive。 |

Cargo 构建不会访问 Java checkout。Java schema、golden frame、迁移状态和性能证据由仓库中的 `scripts/request-header-codec` 资产治理。

## 验证

```powershell
python scripts/request-header-codec/migrate.py check
python scripts/request-header-codec/compare_header_schema.py
cargo test -p rocketmq-macros --lib
cargo test -p rocketmq-protocol --test request_header_codec_v1_ui
cargo test -p rocketmq-protocol --test request_header_codec_v1_wire_snapshot
cargo test -p rocketmq-protocol --test request_header_codec_v3_ui
cargo test -p rocketmq-protocol --test request_header_codec_v2_ui
cargo test -p rocketmq-protocol --test request_header_codec_v2_wire_snapshot
cargo check --locked --offline --manifest-path rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.toml
```

## License

RocketMQ-Rust 使用 Apache License 2.0，详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
