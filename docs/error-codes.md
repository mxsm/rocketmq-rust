# RocketMQ Rust Error Codes

## Contract

`ErrorKind` is the stable semantic identity of a RocketMQ Rust failure. Each kind has one `ErrorSpec` that defines its
stable code, default message, retry class, severity, sensitivity, and observability behavior. Boundary adapters derive a
redacted `BoundaryErrorView` and map that view to remoting, gRPC, HTTP, CLI, callback, or retry behavior; callers must not
invent a second code or stringify a source early.

Code strings are compatibility surfaces. They may be added, but must not be renamed, reused for a different meaning, or
removed without an explicit breaking-version and migration decision.

## Stable code registry

### Network, protocol, and serialization

- `NETWORK_CONNECTION_FAILED`
- `SERIALIZATION_FAILED`
- `PROTOCOL_ERROR`
- `RPC_ERROR`

### Broker, topic, queue, message, route, and cluster

- `AUTHENTICATION_FAILED`
- `CONTROLLER_ERROR`
- `INVALID_PROPERTY`
- `BROKER_NOT_FOUND`
- `BROKER_REGISTRATION_FAILED`
- `BROKER_OPERATION_FAILED`
- `TOPIC_NOT_EXIST`
- `QUEUE_NOT_EXIST`
- `SUBSCRIPTION_GROUP_NOT_EXIST`
- `QUEUE_ID_OUT_OF_RANGE`
- `MESSAGE_TOO_LARGE`
- `MESSAGE_VALIDATION_FAILED`
- `RETRY_LIMIT_EXCEEDED`
- `TRANSACTION_REJECTED`
- `BROKER_PERMISSION_DENIED`
- `NOT_MASTER_BROKER`
- `MESSAGE_LOOKUP_FAILED`
- `QUERY_NOT_FOUND`
- `TOPIC_SENDING_FORBIDDEN`
- `BROKER_ASYNC_TASK_FAILED`
- `REQUEST_BODY_INVALID`
- `REQUEST_HEADER_ERROR`
- `RESPONSE_PROCESS_FAILED`
- `ROUTE_NOT_FOUND`
- `ROUTE_INCONSISTENT`
- `ROUTE_REGISTRATION_CONFLICT`
- `ROUTE_VERSION_CONFLICT`
- `CLUSTER_NOT_FOUND`

### Client and tools

- `CLIENT_NOT_STARTED`
- `CLIENT_ALREADY_STARTED`
- `CLIENT_SHUTTING_DOWN`
- `CLIENT_INVALID_STATE`
- `PRODUCER_NOT_AVAILABLE`
- `CONSUMER_NOT_AVAILABLE`
- `TOOLS_ERROR`
- `FILTER_ERROR`

### Observability

- `OBSERVABILITY_FEATURE_DISABLED`
- `OBSERVABILITY_CONFIG_INVALID`
- `OBSERVABILITY_METRICS_INIT_FAILED`
- `OBSERVABILITY_TRACES_INIT_FAILED`
- `OBSERVABILITY_LOGS_INIT_FAILED`
- `OBSERVABILITY_LOGGING_INIT_FAILED`
- `OBSERVABILITY_LOG_FILTER_INVALID`
- `OBSERVABILITY_SUBSCRIBER_INSTALL_FAILED`
- `OBSERVABILITY_METRICS_SHUTDOWN_FAILED`
- `OBSERVABILITY_TRACES_SHUTDOWN_FAILED`
- `OBSERVABILITY_LOGS_SHUTDOWN_FAILED`

### Storage and configuration

- `STORAGE_READ_FAILED`
- `STORAGE_WRITE_FAILED`
- `STORAGE_CORRUPTED`
- `STORAGE_OUT_OF_SPACE`
- `STORAGE_LOCK_FAILED`
- `CONFIG_PARSE_FAILED`
- `CONFIG_MISSING`
- `CONFIG_INVALID_VALUE`
- `AUTH_CONFIG_INVALID`
- `AUTH_HOT_RELOAD_FAILED`

### Controller and consensus

- `CONTROLLER_NOT_LEADER`
- `CONTROLLER_RAFT_ERROR`
- `CONTROLLER_CONSENSUS_TIMEOUT`
- `CONTROLLER_SNAPSHOT_FAILED`

### Common boundary failures

- `IO_ERROR`
- `ILLEGAL_ARGUMENT`
- `TIMEOUT`
- `INTERNAL`
- `SERVICE_ERROR`
- `INVALID_VERSION_ORDINAL`
- `NOT_INITIALIZED`
- `MISSING_REQUIRED_MESSAGE_PROPERTY`

## Boundary rules

- Remoting response codes remain Java-compatible; the stable RocketMQ Rust code supplements rather than renumbers the
  wire protocol.
- HTTP/gRPC/CLI adapters map from `ErrorKind`/`ErrorSpec`, preserve retry/severity semantics, and expose only the redacted
  `BoundaryErrorView`.
- Sources are retained with `#[source]` or a typed wrapper. Source display text is not a stable code and is not returned
  when it may contain sensitive data.
- `INTERNAL` is reserved for audited invariants. Ordinary validation, configuration, storage, authorization, lifecycle,
  or routing failures use their narrower registered kind.

## Update Checklist

When adding or changing an error:

- [ ] Add or reuse one `ErrorKind`; do not create a legacy alias.
- [ ] Add exactly one `ErrorSpec` with stable code, retry class, severity, sensitivity, and default message.
- [ ] Add the code to this registry and keep every existing code unchanged.
- [ ] Add/update remoting, gRPC, HTTP, CLI, callback, and retry mappings that can observe the error.
- [ ] Preserve the typed source chain and add redaction tests for sensitive context.
- [ ] Add whole-value/spec tests and the affected boundary integration test.
- [ ] Run `python scripts/error_architecture_guard.py` (or `.\scripts\check-error-hygiene.ps1`) and relevant package tests.
