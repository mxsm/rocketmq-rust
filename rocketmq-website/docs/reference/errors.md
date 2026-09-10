---
title: "Errors, statuses, and recovery decisions"
---

# Errors, statuses, and recovery decisions

First identify the boundary that reported the failure. A Rust error code, remoting response code, gRPC payload status, gRPC transport status, and process exit code describe different contracts. Preserve the code and operation context; do not build integrations by matching a human-readable error sentence.

## Which value should a caller inspect?

| Surface | Value to inspect | Interpretation |
| --- | --- | --- |
| Canonical Rust error | `Error::descriptor()` and its stable dotted code | Identifies the declared failure and recovery hint; retains a typed cause for internal diagnosis. |
| Client facade error | `ClientError::descriptor()` or `shared_error()` | Preserves the canonical descriptor instead of flattening the cause into a string. |
| Remoting response | Numeric response code and approved response fields | Many canonical errors can project to the same numeric code; a numeric code is not a unique internal cause. |
| Proxy gRPC | Method response status, item statuses where present, and transport result | A successfully delivered RPC can carry a failed RocketMQ operation. A transport failure may prevent any payload result. |
| Producer result | Outer result, result presence, then `SendResult.send_status` | A returned result can still report a flush/replication timeout. |
| CLI | Process exit status, safe stderr, and command-specific output | Exit status is an automation signal; partial operation results may also require inspection. |

The [error catalog](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/src/catalog.rs) and its component modules own descriptor identities and projections. An outer service boundary can wrap a cause in a service-level descriptor, so a startup failure does not necessarily expose the innermost configuration error code.

## Common canonical errors

The recovery hint is catalog metadata. The action column explains how to combine it with the operation; it is not an automatic retry promise.

| Stable code | Catalog hint | Common cause and next action |
| --- | --- | --- |
| `core.configuration.parse_failed` | `Never` | Invalid input syntax or deserialization. Correct the selected file/format before starting again. |
| `core.configuration.missing` / `core.configuration.invalid` | `Never` | Missing required setting or invalid value/combination. Inspect the owning configuration schema and precedence. |
| `protocol.header.invalid` / `protocol.body.invalid` | `Never` | Malformed fields or body encoding. Fix the request; replaying identical bytes will not repair it. |
| `protocol.request.unsupported` / `protocol.version.unsupported` | `Never` | Selected endpoint or version does not implement the request. Check endpoint, feature, and compatibility scope. |
| `route.topic.not_found` | `RefreshRoute` | Topic absent, unregistered, or not visible through the selected NameServer. Verify provisioning and Broker registration, then refresh within a bounded retry budget. |
| `auth.credentials.invalid` | `RefreshCredentials` | Invalid identity/signature or stale credentials. Correct or rotate credentials; never include their values in diagnostics. |
| `auth.permission.denied` | `Never` | The identity lacks permission for the requested resource/action. Verify policy and resource scope rather than retrying unchanged credentials indefinitely. |
| `transport.admission.queue_saturated` | `Backoff` | Local request admission is full. Reduce concurrency, inspect pending count/bytes, and retry only within the operation's budget. |
| `transport.connection.timeout` / `transport.response.timeout` | `Backoff` | Connection or response deadline elapsed. Determine whether request bytes may have reached the peer before deciding whether a mutation is safe to repeat. |
| `controller.leadership.not_leader` | `RefreshLeader` | Request reached a Controller that is not the current leader. Refresh leader metadata; preserve the original operation identity. |
| `storage.capacity.exhausted` | `OperatorAction` | Storage cannot admit the operation. Inspect free space, retention pressure, and configured limits; retries alone do not create capacity. |
| `storage.read.failed` / `storage.write.failed` | `OperatorAction` | The storage operation failed. Preserve bounded diagnostic context and investigate the I/O/backend state. |
| `storage.state.corrupted` | `OperatorAction` | Storage state violates its expected format/invariants. Isolate the affected recovery path and use the backup/recovery procedure; do not delete format metadata to suppress the error. |

The table is a working reference, not the entire catalog. Broker, Proxy, Controller, Client, auth, observability, and tooling have more specific descriptors in [component catalogs](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-error/src/catalog). Preserve a more specific descriptor when it is already available.

## Descriptor projections: exact examples

These examples are the catalog's declared projections. They do not imply that every HTTP or gRPC endpoint uses the same outer envelope or always returns the listed transport status.

| Canonical descriptor | Remoting | gRPC payload / transport projection | HTTP / CLI projection |
| --- | --- | --- | --- |
| `protocol.header.invalid` | `InvalidParameter (29)` | `BadRequest / InvalidArgument` | `400 / 64` |
| `route.topic.not_found` | `TopicNotExist (17)` | `TopicNotFound / NotFound` | `404 / 66` |
| `auth.credentials.invalid` | `NoPermission (16)` | `Unauthorized / Unauthenticated` | `401 / 77` |
| `auth.permission.denied` | `NoPermission (16)` | `Forbidden / PermissionDenied` | `403 / 77` |
| `transport.admission.queue_saturated` | `SystemBusy (2)` | `TooManyRequests / ResourceExhausted` | `429 / 75` |
| `controller.leadership.not_leader` | `ControllerNotLeader (2007)` | `InternalError / FailedPrecondition` | `409 / 65` |
| `storage.capacity.exhausted` | `SystemError (1)` | `InternalError / ResourceExhausted` | `507 / 65` |

For example, both invalid credentials and denied permissions map to remoting code `16`. A retry implementation that sees only `16` cannot infer that refreshing credentials will solve a policy denial. Likewise, a generic `SystemError` can represent several storage or service failures and is not a safe instruction to replay a write.

The numbers and lightweight enums are defined in [boundary types](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/src/boundary.rs). Adapter code translates them to protocol-specific response types; the error kernel itself does not own networking.

## Send and pull outcomes are not all errors

| Result | Meaning | Caller behavior |
| --- | --- | --- |
| `SendStatus::SendOk` / `SEND_OK` | The chosen send path satisfied its configured response condition | Continue according to the business contract; this does not prove consumer processing. |
| `FlushDiskTimeout` / `FLUSH_DISK_TIMEOUT` | Requested local-flush wait timed out | Treat durability outcome as uncertain. Diagnose disk progress and use idempotent retry logic. |
| `FlushSlaveTimeout` / `FLUSH_SLAVE_TIMEOUT` | Requested replica wait timed out | Inspect replication progress and the selected acknowledgment policy. |
| `SlaveNotAvailable` / `SLAVE_NOT_AVAILABLE` | The required replica was unavailable | Restore the intended topology or make an explicit availability/durability decision. |
| Pull `Found` | Messages were returned | Process before advancing business-completion offsets. |
| Pull `NoNewMsg` / `NoMatchedMsg` | No new messages or no messages matching the selection | Continue polling according to the returned progress and wait policy; do not treat this as a transport outage. |
| Pull `OffsetIllegal` | Requested offset is outside the acceptable queue range | Inspect returned offset guidance, retention, and the consumer's recovery policy before moving progress. |

The canonical result definitions are in [`rocketmq-model::result`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs). A one-way send has no Broker response to inspect. POP receipt/invisible-time errors require the POP acknowledgment model rather than classic pull-offset recovery. See [delivery and retry](../guides/delivery-and-retry.md), [LitePull](../consumer/pull-consumer.md), and [POP](../consumer/pop.md).

## Safe CLI diagnostics

The canonical CLI view emits one line in this form:

```text
ERROR route.topic.not_found: Topic route was not found
```

Default output contains the stable code and fixed public message. Verbose mode may append only descriptor-approved, bounded diagnostic fields; secret-bearing values become `<redacted>`. Neither mode renders source errors, source locations, or backtraces through this view. Use the selected tool's help to determine whether it exposes verbose mode.

| Canonical CLI exit code | Meaning |
| --- | --- |
| `64` | Usage or argument failure |
| `65` | Data or state condition requiring attention |
| `66` | Requested resource not found |
| `69` | Service/resource unavailable |
| `70` | Software/internal failure |
| `75` | Temporary failure |
| `77` | Permission/authentication failure |
| `78` | Configuration failure |

These values describe the shared error view. CLI parsers, wrappers, and specialized commands can define additional exit behavior; for example, a tool-specific preflight denial is not automatically one of these catalog classes. Retain the actual exit code and structured result, not just the last printed line.

## Make a recovery decision

1. Identify the operation, endpoint, response layer, stable error/status, and remaining deadline.
2. Determine whether the operation is read-only, idempotent, or a mutation with uncertain completion. Transport cancellation does not undo remote work.
3. Apply the relevant hint: back off, refresh routing/leader/credentials, or repair configuration/capacity. Bound retries by both attempts and elapsed time.
4. Confirm the original business result or resource state after recovery. Repeated sends must preserve a business event identity and tolerate duplicate delivery.

Collect service role, operation name, approved resource identifiers, elapsed time, and relevant queue/storage/replica progress. Keep credentials, ACL/TLS material, message bodies, arbitrary request objects, and raw configuration values out of shared logs and issue reports. A generic public error intentionally exposes less than the internal cause; use approved diagnostics and [troubleshooting](../operations/troubleshooting.md) rather than weakening redaction.
