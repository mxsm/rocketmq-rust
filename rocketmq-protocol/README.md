# rocketmq-protocol

RocketMQ wire contracts: request/response codes, remoting command frames, custom headers, protocol bodies, compression, and trace encoding. This crate does not open sockets or own a Tokio runtime.

## API and ownership

- `RequestCode`, `RemotingSysResponseCode`, and the [`code`](src/code) module define request and response identity.
- `RemotingCommand`, `EncodedFrame`, and `EncodedFrameHead` represent commands and encoded frame parts.
- `CommandCustomHeader`, `HeaderCodec`, `HeaderMap`, and `FromMap` support typed header encoding and decoding. Derive macros are provided by [rocketmq-macros](../rocketmq-macros/README.md).
- `RpcRequestHeader` and `TopicRequestHeader` provide shared RPC header fields.
- `ProtocolContractViolation` reports invalid protocol contracts; operational failures retain canonical `rocketmq-error` identity.

Business command construction uses [`RemotingCommandFactory`](src/protocol/remoting_command_defaults.rs) with immutable `RemotingCommandDefaults`. An explicit factory can carry independent version/serialization settings without rereading process configuration. The compatibility application-default path is initialized by the application owner. JSON and RocketMQ binary serialization are distinct wire formats; changing a Rust field or enum must preserve its intended wire representation.

The hidden `__request_header_codec` module is support for generated code, not a general integration entry point. Networking, frame limits, connection admission, and request deadlines belong to [rocketmq-transport](../rocketmq-transport/README.md).

## Features

Default features are empty. `simd` enables accelerated decoding paths through `simd-json`; callers still use the same protocol APIs. There is no server binary in this package.

## Validation

Run from the root workspace:

```bash
cargo test -p rocketmq-protocol --test remoting_command_factory --test remoting_wire_golden
cargo test -p rocketmq-protocol --test message_codec_compatibility
cargo bench -p rocketmq-protocol --bench request_header_codec
```

The [`tests`](tests) directory also contains Java compatibility fixtures and compile-fail coverage for header derives. Select the relevant codec and feature combination for a change.

Licensed under [Apache-2.0](../LICENSE-APACHE).
