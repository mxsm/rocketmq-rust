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
| `RemotingSerializable` | Legacy utility | Emits an implementation for the old crate-local serialization trait; incompatible with the current protocol trait. See [Serialization](#serialization). |

All registered production request and response headers use V3. New production headers in this repository must use
V3; V1 and V2 are frozen compatibility adapters only. Both legacy request-header derives are deprecated since
1.0.0 and remain exported for downstream compatibility. They generate `CommandCustomHeader` and `FromMap`
implementations, but do not implement V3's `HeaderCodec` or typed schema.

## Quick start

V3 uses dedicated `#[header(...)]` metadata as the only RocketMQ wire contract. Serde attributes remain independent
and must not be used to infer header keys, defaults, aliases, or flattening.

Consumers need `rocketmq-macros` and `rocketmq-protocol`; this example also uses `cheetah-string`. Within a member
of this repository's workspace, the dependencies can be declared as follows:

```toml
[dependencies]
rocketmq-macros.workspace = true
rocketmq-protocol.workspace = true
cheetah-string.workspace = true
```

This is a small Rust-only example, not the complete production `SendMessageRequestHeader` schema:

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

The generated implementation provides:

- `HeaderCodec` with a stable type ID and typed field/flatten schema;
- `CommandCustomHeader` and `FromMap` compatibility adapters unless `legacy_shim = "manual"` is selected;
- canonical-key and decode-alias resolution with deterministic conflict handling;
- explicit required/default/validation/range behavior;
- decoding from borrowed field sources without first materializing a `HeaderMap`; decoded `String` and
  `CheetahString` fields still copy text into owned values;
- `MapOnly` encoding by default, with direct binary and JSON adapters when `fast` uses the generated shim.

## Supported inputs

V3 accepts named structs, including empty braced structs and structs with generics. Tuple structs, unit structs,
enums, and unions are rejected. Generated implementations preserve generics and where clauses and require the
header type to be `'static`.

Scalar fields support `String`, `CheetahString`, `bool`, `i32`, `i64`, `u32`, `u64`, and the protocol's
`BoundaryType`, plus `Option<T>` of those types. Generic scalar parameters receive a `HeaderValue` bound;
flattened types receive a `HeaderCodec` bound. `HeaderValue` is sealed in `rocketmq-protocol`, so this does not
permit arbitrary downstream wire-value implementations. Other concrete types, such as `Vec<T>`, `&str`, `usize`,
and floats, are not supported as scalar fields. Type classification is syntactic; aliases to supported types are
not automatically resolved by the macro.

## Metadata rules

Container metadata:

| Attribute | Meaning |
| --- | --- |
| `type_id = "..."` | Required stable schema identity, written as a Rust path with at least two segments, no leading `::`, and no generic arguments. |
| `java_class = "..."` | Java peer FQCN. The macro checks its syntax; compatibility tests check the pinned Java schema. Omit it for Rust-only headers. |
| `crate = "path"` | Optional protocol-crate path override. V3 also detects renamed Cargo dependencies automatically. |
| `fast` | Enables direct binary and JSON encoding in the generated compatibility shim. Production use requires correctness and performance review; the macro does not enforce that review. |
| `validate = "path"` | Calls `path(&self)`, returning `Result<(), ProtocolContractViolation>`, before this header layer writes fields and after constructing it during decode. |
| `legacy_shim = "generated"` or `"manual"` | Defaults to `generated`. `manual` suppresses both compatibility impls, including their direct-encoding methods; the caller supplies the adapters. |
| `lookup = "auto"`, `"scan"`, or `"get"` | Accepted metadata; defaults to `auto`. Currently all V3 source decoders scan via `visit_fields_while`, so this option does not select a different lookup algorithm. |

Field metadata:

| Attribute | Meaning |
| --- | --- |
| `required` | Rejects missing input. Concrete required `String` and `CheetahString` fields must also be nonempty on encode and decode. Invalid on `Option<T>`. |
| `default` | Uses `Default::default()` when absent; requires `default_semantic`. |
| `default_with = "path"` | Calls a zero-argument function returning the full field type when absent; requires `default_semantic`. |
| `default_semantic = "literal:<wire-text>"` or `"dynamic:<semantic-id>"` | Describes a default in the schema. It does not calculate or verify the actual default value. The dynamic identifier must be nonempty. |
| `key = "..."` | Canonical wire key. When omitted, underscores are removed and the following character is capitalized: `producer_group` becomes `producerGroup`. |
| `alias = "..."` | Repeatable decode-only key. Encoding emits the canonical key. |
| `alias_conflict = "error"` or `"prefer_canonical"` | Defaults to `error`. See the conflict rules below. |
| `flatten` | Encodes and decodes a nested `HeaderCodec` in the same field namespace. |
| `presence = "always"` or `"any"` | Controls decoding of a flattened field. Required for `Option<Flattened>`; ordinary flattened fields default to `always`. |
| `range = "i32"` or `"i64"` | Restricts `u32` to `0..=i32::MAX` or `u64` to `0..=i64::MAX`, respectively. |
| `binary_order = N` | `u16` encoding/schema order, defaulting to the zero-based source field index. Effective orders must be unique across local scalar and flatten fields. Does not order `HeaderMap` iteration. |
| `java_type = "..."` | Accepted compatibility metadata with type-consistency checks. Registered production schemas require it to be omitted. |

Do not write `java_type` on production fields. V3 infers the ordinary wire kind from the Rust type. Use `range`
for unsigned Rust fields constrained by Java signed integers. Declaring `java_class` on a container requires
`range = "i32"` on each scalar `u32` field and `range = "i64"` on each scalar `u64` field, including optional
fields. Explicit field-level `java_type` also requires the matching range on unsigned fields. Signed Rust fields
must not declare `range`; Rust-only unsigned fields without Java metadata can use their full Rust range.

### Missing values and defaults

Each non-optional scalar field must declare exactly one of `required`, `default`, or `default_with`. Defaults
apply only to absent keys; malformed present values still return an error. An unannotated `Option<T>` decodes
missing input as `None` and omits `None` during encoding. On `Option<T>`, `default` produces
`Some(T::default())`, whereas `default_with` returns the complete `Option<T>` and may choose `None`.

`default_semantic = "literal:32"` does not make `default` return 32. Use a `default_with` function that returns
32, and keep its schema description aligned with its implementation. Default providers are responsible for
returning valid values; generated defaults do not pass through wire-text parsing.

Legacy `#[required]` is temporarily accepted by V3 with a deprecation diagnostic. Use `#[header(required)]`;
declaring both is an error. Serde helper attributes require a Serde derive to register them; V3 itself registers
only `header` and `required`.

### Aliases and flattening

With `alias_conflict = "error"`, multiple present names are accepted if their raw text is identical; different
text produces `ProtocolContractViolation::Conflict` before value parsing. With `prefer_canonical`, the canonical
key wins; if absent, the first present alias in declaration order wins. Unknown input keys are ignored.

Canonical keys and aliases must be nonempty, contain no NUL, and fit the ROCKETMQ `u16` byte-length limit. The
macro rejects duplicate names among local scalar fields. Keep names disjoint across flattened headers as well;
the derive cannot inspect nested types for cross-header collisions.

For `Option<Flattened>`, `presence = "always"` always attempts nested decode and returns `Some` on success.
`presence = "any"` decodes only when a canonical key or alias owned by the nested schema (including its children)
is present; otherwise it returns `None`. A present nested field still triggers all nested required-field checks.
Non-optional flattened fields always decode and cannot use `presence = "any"`. Flattening cannot be combined
with scalar keys, aliases, missing/default policies, `alias_conflict`, `java_type`, or `range`.

### Validation and errors

Prefer `HeaderCodec::encode_into`, `HeaderCodec::decode_from_map` / `decode_from_source`, or
`CommandCustomHeader::try_encode_into_map` to retain typed `ProtocolContractViolation` errors. The generated
legacy `to_map` returns `None` on failure, and `encode_into_map` discards the error. `FromMap` converts failures
to `rocketmq_error::Error`.

Required-string checks and the custom validation hook run before writes by that header layer. Range checks run
as fields are encoded, and nested headers validate when reached. Consequently, a failed `encode_into` or
`try_encode_into_map` can leave earlier fields in the destination. `validate_for_wire` / `check_fields` alone
does not check all field ranges or recursively validate nested headers. The generated `fast` binary and JSON
adapters restore the output buffer's original length on failure.

## Runtime paths and fallback

With the generated shim, a normal header reports `MapOnly`. A `fast` header reports `DirectBinary` and supports
direct JSON fields. The remoting encoder uses shared header access and chooses the path for each command.

For ROCKETMQ encoding, dynamic fields that do not overlap the typed schema can accompany direct binary output;
overlapping canonical keys or aliases use the typed/dynamic map merge. Conflicting typed and dynamic values
produce `DynamicFieldConflict`, even for a field with `alias_conflict = "prefer_canonical"`. The direct JSON path
requires absent extension fields and a header that has not already been materialized into the command.

This fallback is per header and per command. It does not change the wire contract, and it avoids adding a global
branch or environment lookup to every message.

## Migrating legacy headers

V2 metadata is not silently reinterpreted. Review it against the fixed Java schema and convert it explicitly:

| V2 source | V3 decision |
| --- | --- |
| `#[required]` | `#[header(required)]` |
| `serde(rename = "...")` | `#[header(key = "...")]` when it is a wire key |
| `serde(alias = "...")` | `#[header(alias = "...", alias_conflict = "prefer_canonical")]` preserves V2 precedence; choose `error` only as an explicit behavior change |
| `serde(default)` or an implicit non-optional default | Declare `header(default, default_semantic = "literal:...")`; review optional defaults separately |
| `serde(default = "path")` | `#[header(default_with = "path", default_semantic = "...")]` with the actual default semantics |
| `serde(flatten)` on `Option<T>` | `#[header(flatten, presence = "always")]` preserves V2's unconditional nested decode; `any` changes absence behavior |
| `serde(flatten)` on `T` | `#[header(flatten)]` |
| `request_header(validate = "method")` | `#[header(validate = "Self::method")]`, adapting the return type to `ProtocolContractViolation` |
| unsigned field matching Java `int`/`long` | `range = "i32"` / `range = "i64"` |

V2 ignores container-level `serde(rename_all)` and does not apply scalar `Option<T>` default providers during
decode. Review these differences before copying attributes to V3. V3 also has a narrower set of supported
scalar types than V2's `ToString`/`FromStr` path.

Keep V2 only while migrating an existing downstream model. Register new production headers in the typed registry
and checked-in inventory. `request_header_codec_v3_registry` compares that registry with `migration.json` and
the pinned Java contracts. Migration generators and the Java extraction harness have been retired; there is no
active migration guard that automatically discovers and rejects every new source header.

V1 (`RequestHeaderCodec`) is frozen for source compatibility, including its historical parsing and decode quirks.
For example, malformed optional primitive values can become `None`, and malformed non-required primitive values
can fall back to `Default`. V3 returns conversion errors instead. Do not use V1 for new code; migrate existing V1
headers directly to the explicit V3 model.

## Renamed protocol dependency

V2 and V3 resolve `rocketmq-protocol` from the consumer's Cargo manifest, including a dependency renamed to
`protocol_api`. Generated/re-exported environments can override the path explicitly. For V3:

```rust
use rocketmq_macros::RequestHeaderCodecV3;

#[derive(RequestHeaderCodecV3)]
#[header(type_id = "example::Header", crate = "protocol_api")]
struct Header {
    #[header(required)]
    queue_id: i32,
}
```

V2 uses `#[request_header_codec_v2(crate = "protocol_api")]`. V1 still emits `crate::protocol` paths and requires
the legacy consumer layout. The standalone [`tests/fixtures/renamed-consumer`](tests/fixtures/renamed-consumer/)
project checks automatic dependency-name resolution for V2 and V3.

## Serialization

The exported `RemotingSerializable` derive still emits the historical form
`impl crate::protocol::RemotingSerializable for Type { type Output = Self; }`. It does not preserve generics,
resolve a protocol dependency, or generate Serde implementations. The current protocol serialization trait has
no `Output` associated type and requires serialization methods, so this expansion is incompatible with it.

For current protocol types, derive `serde::Serialize` and import
`rocketmq_protocol::protocol::RemotingSerializable` to use `encode`, `serialize_json`, and
`serialize_json_pretty`. The protocol crate supplies a blanket implementation for serializable types.
Likewise, owned deserializable types receive `RemotingDeserializable` through its blanket implementation.

## Crate layout

| Path | Purpose |
| --- | --- |
| [`src/lib.rs`](src/lib.rs) | Public derive entry points and shared parsing helpers. |
| [`src/request_header_codec_v3/`](src/request_header_codec_v3/) | Canonical V3 metadata, semantic model, profile validation, and code generation. |
| [`src/request_header_codec_v3/legacy_v1.rs`](src/request_header_codec_v3/legacy_v1.rs) and [`legacy_v2.rs`](src/request_header_codec_v3/legacy_v2.rs) | Frozen V1/V2 syntax adapters and compatibility code generation over the canonical model. |
| [`src/request_header_codec_v2.rs`](src/request_header_codec_v2.rs) and [`src/request_header_codec_v2/attr.rs`](src/request_header_codec_v2/attr.rs) | Deprecated V2 entry wrapper and public syntax parser; adaptation lives under `legacy_v2/`. |
| [`src/request_header_custom.rs`](src/request_header_custom.rs) | Deprecated V1 parse/wrapper entry forwarding to the frozen compatibility adapter. |
| [`src/remoting_serializable.rs`](src/remoting_serializable.rs) | Historical crate-local serialization expansion. |

No Java checkout is accessed during Cargo builds. Java schemas, golden frames, header registry data, and
benchmark inputs are owned by the protocol crate's
[compatibility fixtures](../rocketmq-protocol/tests/fixtures/request_header_codec/README.md).

## Validation

Select checks relevant to the change from the repository root. The macro crate's unit tests inspect parsing and
expansion; protocol tests compile consumers and exercise runtime behavior.

```powershell
cargo test -p rocketmq-macros --lib
cargo test -p rocketmq-protocol --test request_header_codec_v3_typed_map
cargo test -p rocketmq-protocol --test request_header_codec_v3_registry
cargo test -p rocketmq-protocol --test request_header_codec_v3_ui
cargo test -p rocketmq-protocol --test request_header_codec_runtime_ui
cargo test -p rocketmq-protocol --test request_header_java_compatibility
```

For retained legacy behavior and renamed dependencies:

```powershell
cargo test -p rocketmq-protocol --test request_header_codec_v1_ui
cargo test -p rocketmq-protocol --test request_header_codec_v1_wire_snapshot
cargo test -p rocketmq-protocol --test request_header_codec_v2_ui
cargo test -p rocketmq-protocol --test request_header_codec_v2_wire_snapshot
cargo check --locked --offline --manifest-path rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.toml
```

## License

RocketMQ-Rust is licensed under the Apache License 2.0. See [../LICENSE-APACHE](../LICENSE-APACHE).
