# Remoting processor contract

The remoting processor API has one canonical, unpublished contract. It does not expose implementation-generation modules, aliases, builders, or compatibility adapters.

## Public API

Consumers import processor and transport types directly from `rocketmq_transport::api`:

```rust
use rocketmq_transport::api::{
    HandlerOutcome, RemotingRequest, RequestProcessor, RemotingResponse,
};
```

A processor owns request business logic but does not own a socket, channel, connection lock, or response writer:

```rust
#[derive(Clone)]
struct Processor;

impl RequestProcessor for Processor {
    async fn process(
        &mut self,
        _request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(RemotingResponse::empty_response(0)))
    }
}
```

The transport captures immutable ingress identity, applies admission and security policy, invokes hooks, validates the returned outcome, and exclusively owns terminal response delivery.

## Response and deferred ownership

- `HandlerOutcome::Reply` transfers one affine `RemotingResponse` to the transport.
- `HandlerOutcome::Deferred` transfers a registered deferred response owner.
- `HandlerOutcome::NoReply` records an explicit protocol-level non-response reason.
- One-way requests never gain response-write authority.
- `RemotingResponse` retains byte segments and file leases until writer completion.
- Session shutdown cancels and awaits owned request, deferred, and writer work.

## Embedded dispatch

Broker/Proxy in-process calls use `AuthorizedCommandDispatcher::dispatch_embedded` or `dispatch_embedded_wait_response`. Embedded callers receive `EmbeddedDispatchOutcome` and never reconstruct a network channel or materialize a compatibility command facade.

## Naming policy

Implementation-generation suffixes are prohibited for this contract. Names such as `RequestProcessor`, `TransportServer`, `SessionRegistry`, and `AuthorizedCommandDispatcher` identify the only implementation.

Protocol and storage versions remain explicit when required for interoperability. Examples include `SendMessageV2`, `HeartbeatV2Result`, PROXY protocol v1/v2, and versioned POP retry topics; those are not processor implementation generations.

## Required validation

```text
cargo fmt -p rocketmq-transport -- --check
cargo test -p rocketmq-transport --all-features
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Runtime ownership changes also require the repository runtime audit. Public API changes require regenerating and checking the public API intent and structural snapshot baselines.
