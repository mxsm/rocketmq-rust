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
| `RequestHeaderCodecV2` | Primary | `CommandCustomHeader::to_map` and `FromMap::from` for named Rust structs, with optimized map decoding, required-field checks, `serde(rename)`, `serde(alias)`, and flattened nested headers. |
| `RequestHeaderCodec` | Compatibility | Earlier request-header codec derive with Java-style camelCase keys, `#[required]`, optional fields, primitive parsing, and flattened nested headers. |
| `RemotingSerializable` | Utility | Implements `crate::protocol::RemotingSerializable` for a type. In most current remoting paths, serde-backed blanket implementations are preferred. |

## How It Fits

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

The generated code intentionally targets the protocol contracts used by `rocketmq-remoting`:

- `crate::protocol::command_custom_header::CommandCustomHeader`
- `crate::protocol::command_custom_header::FromMap`
- `crate::protocol::RemotingSerializable`
- `cheetah_string::CheetahString`
- `rocketmq_error::RocketMQError`

Because the expansion uses `crate::protocol...` paths, these macros are intended for crates that expose compatible
protocol modules. Inside this workspace, that is primarily `rocketmq-remoting`.

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
attribute is present. `serde(alias = "...")` values are accepted during decoding for backwards-compatible wire names.

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
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | `RequestHeaderCodec` and `RequestHeaderCodecV2` expansion logic. |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | `RemotingSerializable` derive expansion. |
| [`Cargo.toml`](Cargo.toml) | Proc-macro crate configuration and macro parsing dependencies. |

## Requirements

- Rust `1.85.0` or newer.
- The repository toolchain from [`../rust-toolchain.toml`](../rust-toolchain.toml).
- A consuming crate with compatible `crate::protocol` module paths for generated protocol trait implementations.

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

External use is only practical when the consuming crate exposes the same protocol traits and supporting types expected
by the generated code.

## Validation

Focused checks for this crate:

```bash
cargo test -p rocketmq-macros --lib
```

Because the macros are consumed heavily by `rocketmq-remoting`, validate downstream protocol headers after changing macro
generation logic:

```bash
cargo test -p rocketmq-remoting --lib
```

Workspace-level Rust validation is run from the repository root when Rust code changes:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## License

RocketMQ-Rust is licensed under the Apache License 2.0. See [../LICENSE-APACHE](../LICENSE-APACHE).
