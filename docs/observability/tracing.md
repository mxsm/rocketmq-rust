# Observability Tracing

Tracing uses `tracing`, `tracing-opentelemetry`, and standard W3C trace
context propagation through RocketMQ message properties.

## Broker Configuration

```yaml
traceExporterType: otlp_grpc
traceSampleRatio: 0.01
tracePropagateContext: true
traceRecordMessageId: false
traceRecordMessageKeys: false
traceRecordBodySize: true
```

Use `traceExporterType: log` for local debugging without an OTLP collector.
`traceRecordMessageId` and `traceRecordMessageKeys` stay disabled by default
to avoid recording high-cardinality fields. `traceRecordBodySize` is enabled by
default because it records only the payload size.

When enabled, the message recording flags write these span attributes:

| Config | Span attribute |
| --- | --- |
| `traceRecordMessageId` | `messaging.message.id` |
| `traceRecordMessageKeys` | `messaging.rocketmq.message.keys` |
| `traceRecordBodySize` | `messaging.message.body.size` |

## Span Names

The current instrumentation uses RocketMQ-oriented span names:

| Span | Scope |
| --- | --- |
| `RocketMQ PRODUCER SEND` | Producer send path |
| `RocketMQ BROKER RECEIVE_SEND` | Broker send-message request processing |
| `RocketMQ STORE APPEND` | Store append path |
| `RocketMQ CONSUMER PROCESS` | Consumer listener execution |

Consumer ACK, retry, and DLQ outcomes are recorded as span events.

## Context Propagation

The propagation layer injects and extracts these message properties:

| Property | Purpose |
| --- | --- |
| `traceparent` | W3C trace parent |
| `tracestate` | W3C trace state |
| `baggage` | W3C baggage |

Producer sends inject the current context before serializing request
properties. Broker and consumer paths extract the context and attach it to the
current span.

## Sampling

`traceSampleRatio` accepts values from `0.0` to `1.0`.

| Value | Behavior |
| --- | --- |
| `0.0` | No traces sampled |
| `0.01` | Default 1 percent sampling |
| `1.0` | Full sampling, useful for local debugging |
