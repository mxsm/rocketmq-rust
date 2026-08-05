# rocketmq-macros

[English](README.md) | [简体中文](README-zh_cn.md)

Procedural macros for RocketMQ-Rust protocol types, request headers, and remoting serialization helpers.

`rocketmq-macros` is a small proc-macro crate used by the RocketMQ-Rust workspace to remove repetitive protocol glue
from request and response header definitions. Its primary job is to generate `CommandCustomHeader` and `FromMap`
implementations for RocketMQ remoting headers, preserving Java-compatible wire keys while keeping the Rust structs
typed and maintainable.

This crate is infrastructure, not a runtime component. Most application code should use the higher-level
`rocketmq-remoting`, `rocketmq-client-rust`, `rocketmq-broker`, or service crates instead of depending on these macros
directly.

## Capabilities

| Macro | Status | What it generates |
|-------|--------|-------------------|
| `RequestHeaderCodecV2` | Primary | `CommandCustomHeader::to_map`, allocation-aware `encode_into_map`, and borrowed `FromMap::from` generation for named Rust structs, with deterministic aliases, required-field checks, generics, and flattened nested headers. |
| `RequestHeaderCodec` | Compatibility | Earlier request-header codec derive with Java-style camelCase keys, `#[required]`, optional fields, primitive parsing, and flattened nested headers. |
| `RemotingSerializable` | Utility | Implements `crate::protocol::RemotingSerializable` for a type. In most current remoting paths, serde-backed blanket implementations are preferred. |

## How It Fits

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

The generated code targets the public protocol contracts from `rocketmq-protocol`:

- `rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader`
- `rocketmq_protocol::protocol::command_custom_header::FromMap`
- `rocketmq_protocol::HeaderMap`

The derive resolves `rocketmq-protocol` even when the dependency is renamed in `Cargo.toml`. An explicit path can be
provided for generated or re-exported protocol APIs:

```rust
#[derive(RequestHeaderCodecV2)]
#[request_header_codec_v2(crate = "path::to::protocol_api")]
struct Header {
    queue_id: i32,
}
```

## Quick Start

Use `RequestHeaderCodecV2` on a named struct that represents a RocketMQ remoting header:

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

The derive generates:

- associated string constants for the wire keys;
- `CommandCustomHeader::to_map`, omitting `None` values;
- `CommandCustomHeader::encode_into_map`, writing flattened fields into one destination map;
- `FromMap::from`, converting `CheetahString` map values back into typed fields;
- required-field errors for fields annotated with `#[required]`;
- default values for missing non-required, non-`Option` fields.

## Field Mapping

| Rust field shape | Serialization behavior | Deserialization behavior |
|------------------|------------------------|--------------------------|
| `CheetahString` | Insert directly into the ext-field map. | Read directly, or default when not required. |
| `String` | Convert to `CheetahString`. | Convert back to `String`. |
| Primitive types | Convert with `to_string()`. | Parse with `FromStr`; required fields return a typed header error on parse failure. |
| `Option<T>` | Insert only when `Some`. | Missing values become `None`; present primitive values are parsed. |
| `#[serde(flatten)]` nested header | Merge the nested header map. | Reconstruct by calling the nested type's `FromMap::from`. |

`RequestHeaderCodecV2` uses the field name converted from snake_case to camelCase unless a `serde(rename = "...")`
attribute is present. `serde(alias = "...")` values are checked in declaration order during decoding, after the
canonical key. Canonical values therefore win independently of `HashMap` iteration order.

Invalid combinations are rejected at compile time, including tuple/unit structs, enums, unions, empty or colliding
wire keys, `#[required] Option<T>`, required fields with `serde(default)`, and scalar fields with `serde(flatten)`.
Key collisions inside flattened child headers are checked by the repository schema comparator because a derive macro
cannot inspect another type's fields.

## Required Fields

`#[required]` marks a header field that must be present during `FromMap::from`:

```rust
#[derive(RequestHeaderCodecV2)]
pub struct QueryMessageRequestHeader {
    #[required]
    pub topic: cheetah_string::CheetahString,
    pub key: Option<cheetah_string::CheetahString>,
}
```

This mirrors the intent of Java RocketMQ's `@CFNotNull` annotation. Missing required fields return
`RocketMQError::DeserializeHeaderError` with the generated wire-key name.

## Crate Layout

| Path | Purpose |
|------|---------|
| [`src/lib.rs`](src/lib.rs) | Public proc-macro entry points and shared type helpers. |
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | Legacy `RequestHeaderCodec` expansion logic. |
| [`src/request_header_codec_v2/`](src/request_header_codec_v2/) | V2 attribute parsing, semantic model, validation, and code generation. |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | `RemotingSerializable` derive expansion. |
| [`Cargo.toml`](Cargo.toml) | Proc-macro crate configuration and macro parsing dependencies. |

## Requirements

- Stable Rust `1.95.0`, using the pinned repository toolchain.
- The repository toolchain from [`../rust-toolchain.toml`](../rust-toolchain.toml).
- A direct or renamed `rocketmq-protocol` dependency, or an explicit `request_header_codec_v2(crate = "...")` path.

## Installation

Inside this workspace:

```toml
[dependencies]
rocketmq-macros = { path = "../rocketmq-macros" }
```

For external consumers:

```toml
[dependencies]
rocketmq-macros = "1.0.0"
```

Renamed dependencies are supported without source changes. The standalone fixture under
`tests/fixtures/renamed-consumer` verifies this contract with an offline Cargo check.

## Validation

Focused checks for this crate:

```bash
cargo test -p rocketmq-macros --lib
cargo test -p rocketmq-protocol --test request_header_codec_v2_wire_snapshot
cargo test -p rocketmq-protocol --test request_header_codec_v2_ui
cargo check --offline --manifest-path rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.toml
```

Because the macro is consumed by most protocol headers, validate the protocol crate after changing generation logic:

```bash
cargo test -p rocketmq-protocol --lib
```

Workspace-level Rust validation is run from the repository root when Rust code changes:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## License

RocketMQ-Rust is licensed under the Apache License 2.0. See [../LICENSE-APACHE](../LICENSE-APACHE).
