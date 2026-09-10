---
title: "Error projection and observability ownership"
---

Errors and telemetry serve different purposes. A typed error preserves the failure's identity and approved boundary response; telemetry records how often and where operations occur. A log string should not become either a protocol contract or the input to an automatic retry policy.

## One canonical error, explicit projections

`rocketmq-error` owns the opaque `Error` envelope and immutable `ErrorDescriptor` catalog. Descriptors contain stable dotted codes, fault/classification metadata, fixed public messages, severity, recovery advice, exposure rules, context schemas, and explicit remoting/gRPC/HTTP/CLI projections.

`Error` is not cloneable. `SharedError = Arc<Error>` shares the same typed source, descriptor, and context when several owners need the failure. Component facades preserve this envelope rather than parsing and reconstructing errors from their Display text.

| Boundary | Descriptor-owned projection | Adapter responsibility |
| --- | --- | --- |
| Remoting | `RemotingSpec` | Set the intended numeric response code and safe remark/context |
| gRPC | `GrpcSpec` | Distinguish payload status from transport status |
| HTTP | `HttpSpec` | Apply HTTP status and approved public body |
| CLI | `CliSpec` | Apply process exit status and safe CLI output |

The error kernel does not depend on transport implementations or generated protobuf bindings. Adapters translate its lightweight values into their concrete protocols. A canonical dotted error code is not the same thing as a RocketMQ numeric response code.

For example, `route.topic.not_found` has a fixed public message and a route-refresh recovery hint. A boundary should obtain those values from the descriptor; matching the words “not found” in an arbitrary error string loses both identity and the intended projection.

## Public and diagnostic context

`PublicErrorView` exposes only approved fields. A descriptor with generic exposure emits its fixed message and no dynamic public fields; public exposure still requires each field to be declared public in that descriptor's schema.

Typed causes and diagnostic context remain available for internal diagnosis. Safe public views do not stringify source errors, source locations, or backtraces. `ErrorContext` stores bounded fields and value-free presence markers for secrets, not credential contents. A storage failure response can therefore stay stable even when its internal I/O cause contains a private path.

Do not log a whole request or configuration object to compensate for a redacted public error. Add explicit bounded diagnostic fields that answer the operational question without recording credentials, message bodies, or arbitrary user data.

## Recovery advice is not a retry engine

Catalog hints include `Never`, `Backoff`, `RefreshRoute`, `RefreshLeader`, `SwitchBroker`, `RefreshCredentials`, and `OperatorAction`. The operation owner combines the hint with idempotency, write progress, deadline, and retry budget.

An unavailable response after a mutating request may have an uncertain result. A descriptor cannot determine whether the application's business effect is safe to repeat. Client retry, operator remediation, and consumer redelivery are separate mechanisms; see [delivery and retry](../guides/delivery-and-retry.md).

## Telemetry ownership

The application owns a non-cloneable `TelemetryRuntimeGuard`. Components receive cloneable `TelemetryHandle` capabilities and build role-specific recorders with `from_handle`. A handle cannot shut down providers; once the guard closes, surviving handles stop recording.

SDK providers belong to the guard. Initialization does not install global OpenTelemetry meter/tracer providers, although the tracing subscriber and optional text-map propagator are process-level state. Embedded applications must account for an existing subscriber: required installation can fail, while best-effort installation records its status.

The intended lifecycle is to initialize telemetry with an application service context, inject handles into services, stop those services, explicitly flush/shut down telemetry, then close the runtime. Prometheus needs scoped initialization and awaited shutdown for its listener/tasks. Dropping a guard is not a substitute for inspecting `TelemetryShutdownReport`.

## Signals, features, and cardinality

| Facility | Design boundary |
| --- | --- |
| Console/file logging | Available separately from optional OpenTelemetry; filter resolution takes explicit inputs |
| Metrics | Role-specific instruments and bounded Topic/group label policies |
| Traces | Optional spans and propagation through message property maps using handle policy |
| OTLP | Signal feature plus transport/exporter configuration and a reachable collector |
| Prometheus | Metrics feature, scoped HTTP exporter ownership, and scrape configuration |

The observability crate has no default features. Selecting an exporter at runtime without its compiled feature returns a typed feature-disabled error. `observability` is an alias for metrics/traces support, not proof that every exporter or log signal is enabled.

The client feature `observability` enables traces; `observability-metrics` enables metrics. Client OTLP metrics additionally need `rocketmq-observability/otlp-metrics` in the graph. Do not copy a same-named feature between crates without checking what it forwards.

Use low-cardinality outcomes, service roles, and stable operation names as metric labels. Message IDs, keys, receipt handles, arbitrary addresses, and unbounded Topic/group names can multiply series. Trace context uses the lowercase message property keys `traceparent` and `tracestate`; `TRACEPARENT` and `TRACESTATE` are the Rust constant names. The built-in message propagator excludes arbitrary OpenTelemetry `baggage`. Propagation does not replace a message's business identity or authorization.

## Operational interpretation

Correlate request outcome with route availability, admission pressure, storage/replica progress, and lifecycle state. A healthy telemetry exporter does not imply a healthy Broker, and an exporter outage does not by itself establish message loss. Compile checks establish API availability; collector/scrape tests are needed to demonstrate signal delivery.

## Source map

- [Error kernel](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/README.md).
- [Observability design](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/README.md), [handles](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/handle.rs), [logging/bootstrap](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/logging.rs).
- [Runtime](runtime.md), [security](security.md), [observability configuration](../configuration/observability.md).
