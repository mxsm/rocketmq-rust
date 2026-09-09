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
| `RemotingSerializable` | 旧版工具 | 为旧版 crate 本地序列化 trait 生成实现；与当前协议 trait 不兼容。参见[序列化](#序列化)。 |

仓库中登记的全部生产请求头和响应头都已使用 V3。仓库新增生产 Header 必须使用 V3；V1 和 V2 仅是冻结的兼容适配器。两个旧版请求头 derive 自 1.0.0 起已废弃，仍保留导出以兼容下游代码。它们生成 `CommandCustomHeader` 和 `FromMap` 实现，但不实现 V3 的 `HeaderCodec` 或类型化 schema。

## 快速开始

V3 只把专用 `#[header(...)]` 元数据作为 RocketMQ wire 契约。Serde 属性继续独立服务 JSON/DTO，不能用于推断 Header 的 key、default、alias 或 flatten。

使用方需要依赖 `rocketmq-macros` 和 `rocketmq-protocol`；以下示例还使用了 `cheetah-string`。在本仓库工作空间的成员中，可以这样声明依赖：

```toml
[dependencies]
rocketmq-macros.workspace = true
rocketmq-protocol.workspace = true
cheetah-string.workspace = true
```

以下是一个小型 Rust-only 示例，并非生产 `SendMessageRequestHeader` 的完整 schema：

```rust
use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV3;
use rocketmq_protocol::{CommandCustomHeader, HeaderCodec, HeaderMap, ProtocolContractViolation};

#[derive(Debug, RequestHeaderCodecV3)]
#[header(type_id = "example::MessageHeader")]
struct MessageHeader {
    #[header(required)]
    producer_group: CheetahString,
    #[header(required)]
    topic: CheetahString,
    #[header(default, default_semantic = "literal:0")]
    sys_flag: i32,
    batch: Option<bool>,
}

fn main() -> Result<(), ProtocolContractViolation> {
    let header = MessageHeader {
        producer_group: "example-producer".into(),
        topic: "example-topic".into(),
        sys_flag: 0,
        batch: None,
    };
    let mut fields = HeaderMap::new();
    header.try_encode_into_map(&mut fields)?;
    assert_eq!(fields.get("producerGroup").map(|value| value.as_str()), Some("example-producer"));
    assert_eq!(fields.get("sysFlag").map(|value| value.as_str()), Some("0"));
    assert!(!fields.contains_key("batch"));

    fields.remove("sysFlag");
    let decoded = MessageHeader::decode_from_map(&fields)?;
    assert_eq!(decoded.sys_flag, 0);
    assert_eq!(decoded.topic, header.topic);
    Ok(())
}
```

生成结果包括：

- 带稳定类型 ID 和字段/flatten schema 的 `HeaderCodec`；
- `CommandCustomHeader` 与 `FromMap` 兼容适配器，除非指定 `legacy_shim = "manual"`；
- canonical key 与 decode alias 的确定性解析和冲突处理；
- 显式 required/default/validation/range 语义；
- 直接从借用的字段源解码，无需先构造 `HeaderMap`；解码后的 `String` 和 `CheetahString` 字段仍会复制文本并拥有其所有权；
- 默认 `MapOnly` 编码；`fast` 使用生成的兼容适配器时，同时提供直接二进制和 JSON 编码。

## 支持的输入

V3 接受具名字段结构体，包括使用花括号的空结构体和泛型结构体。元组结构体、单元结构体、枚举和联合体均不支持。生成的实现保留泛型及 where 子句，并要求 Header 类型满足 `'static`。

标量字段支持 `String`、`CheetahString`、`bool`、`i32`、`i64`、`u32`、`u64`、协议使用的 `BoundaryType`，以及这些类型的 `Option<T>`。泛型标量参数会添加 `HeaderValue` 约束；展平类型会添加 `HeaderCodec` 约束。`HeaderValue` 在 `rocketmq-protocol` 中是封闭 trait，因此不能通过泛型支持任意下游 wire value 实现。`Vec<T>`、`&str`、`usize` 和浮点数等其他具体类型不支持作为标量字段。宏根据语法判断类型，不会自动解析指向受支持类型的类型别名。

## 元数据规则

容器属性：

| 属性 | 含义 |
| --- | --- |
| `type_id = "..."` | 必填的稳定 schema 标识，使用至少包含两段的 Rust 路径，不允许以 `::` 开头或包含泛型参数。 |
| `java_class = "..."` | Java 对应类型的 FQCN。宏检查名称语法，兼容性测试检查固定的 Java schema。Rust-only Header 不填写。 |
| `crate = "path"` | 可选的 protocol crate 路径覆盖。V3 也会自动识别重命名后的 Cargo 依赖。 |
| `fast` | 在生成的兼容适配器中启用直接二进制和 JSON 编码。生产使用需要正确性和性能审查；宏本身不强制检查审查结果。 |
| `validate = "path"` | 调用返回 `Result<(), ProtocolContractViolation>` 的 `path(&self)`；在当前 Header 层写入字段前，以及解码构造该层后执行。 |
| `legacy_shim = "generated"` 或 `"manual"` | 默认为 `generated`。`manual` 禁止生成两个兼容实现及其中的直接编码方法，由使用方提供适配器。 |
| `lookup = "auto"`、`"scan"` 或 `"get"` | 可接受的元数据，默认为 `auto`。当前所有 V3 字段源解码器均通过 `visit_fields_while` 扫描，该选项不会切换查找算法。 |

字段属性：

| 属性 | 含义 |
| --- | --- |
| `required` | 拒绝缺失的输入。具体类型为 `String` 或 `CheetahString` 的必填字段在编码和解码时还必须非空。不能用于 `Option<T>`。 |
| `default` | 缺失时使用 `Default::default()`；必须声明 `default_semantic`。 |
| `default_with = "path"` | 缺失时调用无参函数，其返回值为完整字段类型；必须声明 `default_semantic`。 |
| `default_semantic = "literal:<wire-text>"` 或 `"dynamic:<semantic-id>"` | 在 schema 中描述默认值，不会计算或校验实际默认值。dynamic 标识符不能为空。 |
| `key = "..."` | canonical wire key。省略时删除下划线并将其后字符转为大写：`producer_group` 变为 `producerGroup`。 |
| `alias = "..."` | 可重复声明的解码专用键。编码输出 canonical key。 |
| `alias_conflict = "error"` 或 `"prefer_canonical"` | 默认为 `error`。详见下文冲突规则。 |
| `flatten` | 在同一字段命名空间中编码和解码嵌套的 `HeaderCodec`。 |
| `presence = "always"` 或 `"any"` | 控制展平字段的解码。`Option<Flattened>` 必须显式声明；普通展平字段默认为 `always`。 |
| `range = "i32"` 或 `"i64"` | 分别将 `u32` 限制到 `0..=i32::MAX`，或将 `u64` 限制到 `0..=i64::MAX`。 |
| `binary_order = N` | `u16` 类型的编码/schema 顺序，默认为从零开始的源字段索引。本地标量字段和展平字段的最终顺序值必须唯一。不会控制 `HeaderMap` 的迭代顺序。 |
| `java_type = "..."` | 可接受的兼容元数据，会检查类型一致性。已登记的生产 schema 要求省略该属性。 |

生产字段不填写 `java_type`。V3 会根据 Rust 类型推断普通 wire kind。受 Java 有符号整数范围约束的无符号 Rust 字段使用 `range`。容器声明 `java_class` 后，每个标量 `u32` 字段都必须声明 `range = "i32"`，每个标量 `u64` 字段都必须声明 `range = "i64"`，可选字段也不例外。字段显式声明 `java_type` 时，无符号字段同样需要匹配的范围。有符号 Rust 字段禁止声明 `range`；不含 Java 元数据的 Rust-only 无符号字段可以使用完整的 Rust 数值范围。

### 缺失值与默认值

每个非可选标量字段必须且只能声明 `required`、`default` 或 `default_with` 中的一种策略。默认值仅用于缺失的键；已提供但格式错误的值仍会返回错误。不加注解的 `Option<T>` 在输入缺失时解码为 `None`，编码时省略 `None`。对于 `Option<T>`，`default` 生成 `Some(T::default())`；`default_with` 则返回完整的 `Option<T>`，可以返回 `None`。

`default_semantic = "literal:32"` 不会使 `default` 返回 32。应通过 `default_with` 指定返回 32 的函数，并保持 schema 描述与实现一致。默认值提供函数负责返回有效值；生成的默认值不会经过 wire 文本解析。

V3 暂时接受旧版 `#[required]`，但会发出弃用诊断。应使用 `#[header(required)]`；同时声明两者会报错。Serde 辅助属性需要由 Serde derive 注册；V3 本身只注册 `header` 和 `required`。

### 别名与展平

使用 `alias_conflict = "error"` 时，多个名称同时存在且原始文本完全相同则接受；文本不同则在值解析前返回 `ProtocolContractViolation::Conflict`。使用 `prefer_canonical` 时，canonical key 优先；若其缺失，则按声明顺序选取第一个存在的别名。未知输入键会被忽略。

canonical key 和别名必须非空、不包含 NUL，并满足 ROCKETMQ 的 `u16` 字节长度限制。宏会拒绝本地标量字段中的重复名称。展平 Header 之间也应保持名称互不冲突；derive 无法检查嵌套类型中的跨 Header 冲突。

对于 `Option<Flattened>`，`presence = "always"` 始终尝试嵌套解码，成功后返回 `Some`。`presence = "any"` 仅在嵌套 schema（包括其子级）拥有的 canonical key 或别名存在时解码，否则返回 `None`。只要嵌套字段存在，仍会触发全部嵌套必填字段检查。非可选展平字段始终解码，不能使用 `presence = "any"`。展平不能与标量键、别名、缺失值/默认值策略、`alias_conflict`、`java_type` 或 `range` 组合。

### 校验与错误

优先使用 `HeaderCodec::encode_into`、`HeaderCodec::decode_from_map` / `decode_from_source` 或 `CommandCustomHeader::try_encode_into_map`，以保留类型化的 `ProtocolContractViolation` 错误。生成的旧版 `to_map` 在失败时返回 `None`，`encode_into_map` 则丢弃错误。`FromMap` 将失败转换为 `rocketmq_error::Error`。

必填字符串检查和自定义校验钩子在当前 Header 层写入前执行。范围检查在字段编码时执行，嵌套 Header 则在处理到该字段时校验。因此，`encode_into` 或 `try_encode_into_map` 失败后，目标中可能保留已写入的字段。单独调用 `validate_for_wire` / `check_fields` 不会检查全部字段范围，也不会递归校验嵌套 Header。生成的 `fast` 二进制和 JSON 适配器会在失败时恢复输出缓冲区的原始长度。

## 运行路径与回退

使用生成的兼容适配器时，普通 Header 返回 `MapOnly`；`fast` Header 返回 `DirectBinary`，并支持直接输出 JSON 字段。Remoting 编码器通过共享访问使用 Header，并按命令选择编码路径。

对于 ROCKETMQ 编码，与类型化 schema 不重叠的动态字段可以和直接二进制输出一起编码；与 canonical key 或别名重叠时，使用类型化字段与动态字段的 map 合并逻辑。类型化字段与动态字段的值冲突时返回 `DynamicFieldConflict`，即使字段声明了 `alias_conflict = "prefer_canonical"` 也不例外。直接 JSON 路径要求扩展字段尚不存在，且 Header 尚未物化到命令中。

回退按 Header 和命令生效，不改变 wire 契约，也不会在每条消息上增加全局开关或环境变量查询。

## 迁移旧 Header

V2 元数据不会被静默解释成 V3。必须对照固定 Java schema 审核后显式转换：

| V2 来源 | V3 决策 |
| --- | --- |
| `#[required]` | `#[header(required)]` |
| `serde(rename = "...")` | 确认是 wire key 后改为 `#[header(key = "...")]` |
| `serde(alias = "...")` | `#[header(alias = "...", alias_conflict = "prefer_canonical")]` 保留 V2 优先级；选择 `error` 应作为显式行为变更 |
| `serde(default)` 或隐式非可选默认值 | 声明 `header(default, default_semantic = "literal:...")`；可选字段的默认值需要单独审核 |
| `serde(default = "path")` | 使用 `#[header(default_with = "path", default_semantic = "...")]` 并描述实际默认值语义 |
| `Option<T>` 上的 `serde(flatten)` | `#[header(flatten, presence = "always")]` 保留 V2 无条件嵌套解码行为；`any` 会改变缺失时的行为 |
| `T` 上的 `serde(flatten)` | `#[header(flatten)]` |
| `request_header(validate = "method")` | 改为 `#[header(validate = "Self::method")]`，并将返回类型适配为 `ProtocolContractViolation` |
| 对应 Java `int`/`long` 的无符号字段 | `range = "i32"` / `range = "i64"` |

V2 忽略容器级 `serde(rename_all)`，解码时也不使用标量 `Option<T>` 的默认值提供函数。将属性复制到 V3 前应审核这些差异。与 V2 的 `ToString`/`FromStr` 路径相比，V3 支持的标量类型范围也更窄。

V2 只应用于尚未完成迁移的既有下游模型。新增生产 Header 应登记到类型化 registry 和仓库内的类型清单。`request_header_codec_v3_registry` 对照 `migration.json` 和固定的 Java 契约检查该 registry。迁移生成器和 Java 提取工具已退役；当前不存在自动发现并拒绝所有新增源码 Header 的 migration guard。

V1（`RequestHeaderCodec`）为源码兼容而冻结，包括其历史解析和解码特殊行为。例如，格式错误的可选基础类型值可能变为 `None`，格式错误的非必填基础类型值可能回退到 `Default`。V3 则返回转换错误。不要将 V1 用于新代码；现有 V1 Header 应直接迁移到显式的 V3 model。

## 重命名 Protocol 依赖

V2 和 V3 从使用方的 Cargo manifest 解析 `rocketmq-protocol`，包括重命名为 `protocol_api` 的依赖。生成代码或 re-export 场景可显式覆盖路径。V3 示例：

```rust
use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "example::Header", crate = "protocol_api")]
struct Header {
    #[header(required)]
    queue_id: i32,
}
```

V2 使用 `#[request_header_codec_v2(crate = "protocol_api")]`。V1 仍生成 `crate::protocol` 路径，要求使用方保留旧版布局。独立项目 [`tests/fixtures/renamed-consumer`](tests/fixtures/renamed-consumer/) 检查 V2 和 V3 自动解析依赖名称的能力。

## 序列化

导出的 `RemotingSerializable` derive 仍生成历史形式 `impl crate::protocol::RemotingSerializable for Type { type Output = Self; }`。它不保留泛型、不解析协议依赖，也不生成 Serde 实现。当前协议序列化 trait 不包含 `Output` 关联类型，并要求实现序列化方法，因此该展开结果与之不兼容。

当前协议类型应派生 `serde::Serialize`，并导入 `rocketmq_protocol::protocol::RemotingSerializable`，以使用 `encode`、`serialize_json` 和 `serialize_json_pretty`。协议 crate 为可序列化类型提供了 blanket implementation（泛型覆盖实现）。同样，满足拥有所有权的反序列化约束的类型会通过泛型覆盖实现获得 `RemotingDeserializable`。

## Crate 结构

| 路径 | 用途 |
| --- | --- |
| [`src/lib.rs`](src/lib.rs) | 公开 derive 入口和共享解析辅助函数。 |
| [`src/request_header_codec_v3/`](src/request_header_codec_v3/) | canonical V3 元数据、语义模型、profile 校验和代码生成。 |
| [`src/request_header_codec_v3/legacy_v1.rs`](src/request_header_codec_v3/legacy_v1.rs) 与 [`legacy_v2.rs`](src/request_header_codec_v3/legacy_v2.rs) | 基于 canonical model 的冻结 V1/V2 语法适配器和兼容代码生成。 |
| [`src/request_header_codec_v2.rs`](src/request_header_codec_v2.rs) 与 [`src/request_header_codec_v2/attr.rs`](src/request_header_codec_v2/attr.rs) | 已废弃的 V2 入口封装和公开语法解析器；适配逻辑位于 `legacy_v2/`。 |
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | 已废弃的 V1 parse/wrapper entry，转发到冻结兼容适配器。 |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | 历史 crate 本地序列化展开逻辑。 |

Cargo 构建不会访问 Java checkout。Java schema、golden frame、请求头注册数据和基准测试输入保存在协议 crate 的[兼容性夹具目录](../rocketmq-protocol/tests/fixtures/request_header_codec/README.md)中。

## 验证

从仓库根目录选择与变更相关的检查。宏 crate 的单元测试检查解析和展开逻辑；协议测试编译使用方代码并验证运行时行为。

```powershell
cargo test -p rocketmq-macros --lib
cargo test -p rocketmq-protocol --test request_header_codec_v3_typed_map
cargo test -p rocketmq-protocol --test request_header_codec_v3_registry
cargo test -p rocketmq-protocol --test request_header_codec_v3_ui
cargo test -p rocketmq-protocol --test request_header_codec_runtime_ui
cargo test -p rocketmq-protocol --test request_header_java_compatibility
```

旧版兼容行为和依赖重命名检查：

```powershell
cargo test -p rocketmq-protocol --test request_header_codec_v1_ui
cargo test -p rocketmq-protocol --test request_header_codec_v1_wire_snapshot
cargo test -p rocketmq-protocol --test request_header_codec_v2_ui
cargo test -p rocketmq-protocol --test request_header_codec_v2_wire_snapshot
cargo check --locked --offline --manifest-path rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.toml
```

## License

RocketMQ-Rust 使用 Apache License 2.0，详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
