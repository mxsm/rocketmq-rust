# Core capability contracts

This document defines the public behavioral boundary for the four crates that
carry cross-cutting runtime, storage, error, and security capabilities. The
public API documentation is compiled with `deny(missing_docs)` in each crate;
this page records the invariants that are easier to understand at crate scope.

## Storage capabilities

Crate: `rocketmq-store-api`

- Ownership: append and read values own their byte ranges or carry a backend
  lease for the complete lifetime of a borrowed storage view. A derived-record
  cursor cannot acknowledge a primary-log append.
- Thread safety: capability traits state their `Send` and `Sync` requirements.
  Concrete engines retain their own synchronization; callers do not receive
  mutable backend internals.
- Cancellation: cancelling an append or checkpoint future does not authorize a
  caller to delete a WAL, mapped file, database, checkpoint, or persistent
  volume. The owning backend completes or reports its durable watermark.
- Errors and retry: `StoreOperation` and `StoreErrorKind` preserve operation,
  component, retry classification, and source. `AppendStatus` distinguishes an
  accepted append from the durability level reached by `AppendReceipt`.
- Resource limits: queue depth, retained bytes, flush backlog, replication
  backlog, and watermarks are explicit values. A capacity rejection is not
  converted into a successful append.
- Compatibility: RocketMQ wire response codes and persisted record layouts are
  mapped by adapters outside this crate. Internal compatibility traits are not
  part of the capability boundary.
- Performance assumptions: append/read capabilities avoid an unconditional
  copy only when ownership remains explicit. No interface promises a particular
  throughput or latency.
- Failure modes: storage unavailable, page-cache pressure, flush timeout,
  replica timeout, corruption, and destructive checkpoint restoration remain
  distinct outcomes.

## Runtime capabilities

Crate: `rocketmq-runtime`

- Ownership: one `RuntimeOwner` owns the Tokio runtime. Production components
  receive a `ChildServiceContext`, `TaskGroup`, `ScheduledTaskGroup`, or
  `BlockingExecutor`; they do not create detached process runtimes.
- Thread safety: task registration, lifecycle state, resource budgets, and
  shutdown reports are synchronized by their owning types. A budget permit is
  released exactly once when its RAII guard is dropped.
- Cancellation: shutdown closes admission, propagates cancellation, and awaits
  owned work until an explicit `ShutdownDeadline`. Work still running after the
  deadline is reported rather than silently detached.
- Errors and retry: runtime creation, queue admission, blocking execution, and
  lifecycle failures use typed runtime errors. Retrying is a caller decision;
  the runtime does not replay arbitrary work.
- Resource limits: task groups and queues expose item, byte, age, rate, control
  reserve, blocking-thread, and blocking-queue limits. Child budgets cannot
  exceed an ancestor.
- Compatibility: `RuntimeContext` is a test and migration harness.
  `RocketMQRuntime` is a deprecated compatibility boundary; new production code
  uses `RuntimeOwner`.
- Performance assumptions: blocking work uses the bounded blocking executor.
  The runtime does not promise scheduling latency, fairness, or throughput.
- Failure modes: closed admission, queue full, deadline exceeded, panic,
  incomplete shutdown, and a blocking task that remains running are observable
  separately.

## Error capabilities

Crate: `rocketmq-error`

- Ownership: a typed error owns its safe context and optional source chain.
  Boundary views borrow immutable data and do not mutate the original error.
- Thread safety: shared errors and source chains used across tasks are `Send`
  and `Sync`; boundary mapping is pure and does not depend on global mutable
  state.
- Cancellation: cancellation and timeout remain distinct error kinds. Mapping a
  timeout never implies that the underlying operation was rolled back.
- Errors and retry: each error maps to a stable kind, code, category, recovery
  class, severity, remoting response, gRPC status, HTTP status, and CLI exit
  code. Retry policy is explicit and never inferred from display text.
- Redaction: debug and boundary views exclude credential, token, ACL, TLS,
  message-body, and raw configuration values. Context fields are admitted by
  the error hygiene guard.
- Resource limits: error context is bounded metadata; errors must not retain
  unbounded payloads or request objects.
- Compatibility: protocol-facing numeric codes are compatibility surfaces.
  Internal enum organization and constructors can change when every caller is
  migrated.
- Performance assumptions: classification and mapping are allocation-light,
  but no latency claim is attached to error conversion.
- Failure modes: serialization, protocol, network, storage, authentication,
  authorization, controller, observability, and tooling errors retain their
  owning category.

## Security capabilities

Crate: `rocketmq-security-api`

- Ownership: request views borrow protocol data for the authorization call.
  Secrets are acquired through a provider and are not copied into request
  diagnostics. Maintenance grants have private fields and can only be created
  by validated policy.
- Thread safety: authorizers, authenticators, signers, and secret providers
  declare the concurrency bounds required by service composition.
- Cancellation: cancellation does not weaken authorization. An incomplete
  policy load, secret rotation, or maintenance decision fails closed.
- Errors and retry: authentication, authorization, signing, secret-provider,
  and maintenance-policy failures remain typed. Only provider-defined transient
  failures are candidates for bounded retry.
- Redaction: secret material, signatures, credentials, certificates, message
  bodies, and full request fields are never included in error display or
  operational labels.
- Resource limits: request bodies are borrowed, maintenance deadlines and
  budgets are validated, and policy-controlled resource scopes are explicit.
- Compatibility: wire headers remain owned by `rocketmq-protocol`; this crate
  defines runtime-neutral security meaning and accepts no protocol-number
  renumbering.
- Performance assumptions: authorization can be called on request paths, so
  implementations must avoid blocking I/O unless their composition boundary
  explicitly routes it through the blocking executor.
- Failure modes: missing identity, invalid signature, expired timestamp,
  unsupported resource pattern, denied policy, stale fencing token, and secret
  provider failure remain distinguishable.

## Verification

The documentation gate for these contracts is:

```bash
cargo doc -p rocketmq-store-api --no-deps
cargo doc -p rocketmq-runtime --no-deps
cargo doc -p rocketmq-error --no-deps
cargo doc -p rocketmq-security-api --no-deps
```

Because each crate uses `deny(missing_docs)`, any newly exported undocumented
module, item, variant, field, method, or associated type fails compilation.
