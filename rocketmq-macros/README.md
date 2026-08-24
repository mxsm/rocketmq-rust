# rocketmq-macros

[English](README.md) | [简体中文](README-zh_cn.md)

Procedural macros for RocketMQ-Rust protocol types and remoting headers.

`rocketmq-macros` is build-time infrastructure. Most application code should use the higher-level client, broker,
or remoting crates instead of depending on it directly.

## Request-header derives

| Macro | Status | Purpose |
| --- | --- | --- |
| `RequestHeaderCodecV3` | Recommended | Generates typed map/source codecs, wire schema, validation, key resolution, compatibility adapters, and optional reviewed direct encoding. |
| `RequestHeaderCodecV2` | Deprecated | Frozen compatibility adapter for the hardened V2 wire contract. No production header may newly adopt it. |
| `RequestHeaderCodec` | Deprecated | Frozen compatibility adapter that preserves the original request-header quirks for downstream source compatibility. |
| `RemotingSerializable` | Utility | Implements the remoting serialization helper for a type. |

All registered production request and response headers use V3. All new request-header code must use V3; V1 and V2
are frozen compatibility adapters only. Legacy derives remain callable for at least one release window and will only
be removed in a future breaking release.

## Quick start

V3 uses dedicated `#[header(...)]` metadata as the only RocketMQ wire contract. Serde attributes remain independent
and must not be used to infer header keys, defaults, aliases, or flattening.

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

The generated implementation provides:

- `HeaderCodec` with a stable type ID and typed field/flatten schema;
- `CommandCustomHeader` and `FromMap` compatibility adapters;
- canonical-key and decode-alias resolution with deterministic conflict handling;
- explicit required/default/validation/range behavior;
- zero-copy borrowed field-source decoding where the input supports it;
- `MapOnly` encoding by default and generated direct encoding only for explicitly reviewed `fast` headers.

## Metadata rules

Container metadata:

| Attribute | Meaning |
| --- | --- |
| `type_id = "..."` | Required stable Rust schema identity. |
| `java_class = "..."` | Java oracle FQCN when the Rust type has a Java peer. Omit it for Rust-only headers. |
| `crate = "path"` | Optional protocol-crate override, including renamed dependencies. |
| `fast` | Enables generated direct encoding only after correctness and performance review. |
| `validate = "path"` | Runs a typed validation hook before the first encode mutation and after decode. |
| `legacy_shim = "manual"` | Avoids duplicate compatibility impls when an audited manual adapter exists. |

Field metadata:

| Attribute | Meaning |
| --- | --- |
| `required` | Missing input is an error. |
| `default` / `default_with = "path"` | Explicit missing-field behavior; pair it with `default_semantic`. |
| `key = "..."` | Canonical wire key. The Rust field name is used when omitted. |
| `alias = "..."` | Decode-only legacy key. V3 never emits aliases. |
| `flatten, presence = "always|any"` | Nested header inheritance/presence contract. |
| `range = "i32|i64"` | Restricts an unsigned Rust field to the corresponding Java signed domain. |

Do not write `java_type` on production fields. V3 infers the ordinary wire kind from the Rust type. Use `range`
only for unsigned Rust fields that are constrained by a Java signed `int` or `long`; signed Rust fields and
Rust-only unsigned fields do not need it.

## Runtime paths and fallback

V3 is the production default because every registered header implements its typed codec and object-safe
compatibility adapter. A normal header reports `MapOnly`; the remoting encoder materializes canonical extension
fields without relying on mutable `Arc` access. A reviewed hot header may report `DirectBinary` for ROCKETMQ frames
and may provide direct JSON fields. If a command already contains materialized or dynamic fields, the same typed
schema resolves collisions and the map path remains authoritative.

This fallback is per header and per command. It does not change the wire contract, and it avoids adding a global
branch or environment lookup to every message.

## Migrating legacy headers

V2 metadata is not silently reinterpreted. Review it against the fixed Java schema and convert it explicitly:

| V2 source | V3 decision |
| --- | --- |
| `#[required]` | `#[header(required)]` |
| `serde(rename = "...")` | `#[header(key = "...")]` when it is a wire key |
| `serde(alias = "...")` | `#[header(alias = "...")]` with an explicit conflict policy when required |
| `serde(default)` | `#[header(default, default_semantic = "literal:...")]` or reviewed `default_with` |
| `serde(flatten)` | `#[header(flatten, presence = "always|any")]` |
| unsigned field matching Java `int`/`long` | `range = "i32"` / `range = "i64"` |

Keep V2 only while migrating an existing downstream model. New RocketMQ-Rust production headers are rejected by
the migration guard unless they use V3 and are registered in the checked-in schema inventory.

V1 (`RequestHeaderCodec`) is frozen for source compatibility, including its historical parsing and decode quirks.
Do not use it for new code; migrate existing V1 headers directly to the explicit V3 model.

## Renamed protocol dependency

The derive resolves `rocketmq-protocol` through Cargo metadata. Generated/re-exported environments can override it:

```rust
#[derive(RequestHeaderCodecV3)]
#[header(type_id = "example::Header", crate = "protocol_api")]
struct Header {
    #[header(required)]
    queue_id: i32,
}
```

The standalone `tests/fixtures/renamed-consumer` project verifies both the V3 path and retained V2 compatibility.

## Crate layout

| Path | Purpose |
| --- | --- |
| [`src/lib.rs`](src/lib.rs) | Public derive entry points and shared parsing helpers. |
| [`src/request_header_codec_v3/`](src/request_header_codec_v3/) | Canonical V3 metadata, semantic model, profile validation, and code generation. |
| [`src/request_header_codec_v3/legacy_v1.rs`](src/request_header_codec_v3/legacy_v1.rs) and [`legacy_v2.rs`](src/request_header_codec_v3/legacy_v2.rs) | Frozen V1/V2 syntax adapters and compatibility code generation over the canonical model. |
| [`src/request_header_codec_v2/`](src/request_header_codec_v2/) | Deprecated V2 public syntax parser and adapter. |
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | Deprecated V1 parse/wrapper entry forwarding to the frozen compatibility adapter. |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | Remoting serialization derive. |

No Java checkout is accessed during Cargo builds. Java schemas, golden frames, migration state, and performance
evidence are governed by the repository's `scripts/request-header-codec` assets.

## Validation

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

RocketMQ-Rust is licensed under the Apache License 2.0. See [../LICENSE-APACHE](../LICENSE-APACHE).
