# rocketmq-runtime

[![Crates.io](https://img.shields.io/crates/v/rocketmq-runtime.svg)](https://crates.io/crates/rocketmq-runtime)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE-APACHE)

`rocketmq-runtime` is the small runtime adapter used by the
[rocketmq-rust](https://github.com/mxsm/rocketmq-rust) workspace when a
component needs to own a dedicated Tokio multi-thread runtime. It provides a
consistent wrapper for creating named worker pools, borrowing Tokio handles,
running fixed-rate background tasks, and shutting owned runtimes down from
RocketMQ components.

This crate intentionally stays narrow. It does not replace Tokio, implement a
task manager, or own component lifecycle policy. It gives broker, client, and
future workspace components a shared runtime ownership primitive that keeps
thread naming, handle access, and shutdown semantics consistent.

[中文文档](README-zh_cn.md)

## Architecture

![rocketmq-runtime architecture](../resources/runtime-architecture.svg)

`rocketmq-runtime` sits between RocketMQ components and Tokio:

- Workspace components request a named runtime through `RocketMQRuntime`.
- `RocketMQRuntime::new_multi` builds a Tokio multi-thread runtime with IO,
  timer, and other Tokio drivers enabled.
- Components use `get_handle()` or `get_runtime()` when they need direct Tokio
  access.
- Fixed-rate helpers spawn repeating synchronous closures onto the owned
  runtime.
- Shutdown APIs consume the wrapper and delegate to Tokio background or timeout
  shutdown.

## Capabilities

- Create named Tokio multi-thread runtimes with an explicit worker-thread
  count.
- Borrow the underlying `tokio::runtime::Handle` for task spawning.
- Borrow the underlying `tokio::runtime::Runtime` for lower-level integration.
- Run fixed-rate background tasks with optional initial delay.
- Support both immutable `Fn` and mutable `FnMut` scheduled closures.
- Shut down runtime ownership in the background or with a bounded timeout.
- Keep the crate dependency surface intentionally small: Tokio only.

## Quick Start

```toml
[dependencies]
rocketmq-runtime = { path = "../rocketmq-runtime" }
```

Create a runtime, spawn async work, schedule periodic maintenance, and shut the
runtime down when the owner is finished:

```rust
use std::time::Duration;

use rocketmq_runtime::RocketMQRuntime;

let runtime = RocketMQRuntime::new_multi(4, "rocketmq-worker");

runtime.get_handle().spawn(async {
    // Run component-specific async work.
});

runtime.schedule_at_fixed_rate(
    || {
        // Run lightweight periodic work.
    },
    Some(Duration::from_secs(1)),
    Duration::from_secs(5),
);

runtime.shutdown_timeout(Duration::from_secs(3));
```

For mutable scheduler state, use `schedule_at_fixed_rate_mut`:

```rust
use std::time::Duration;

use rocketmq_runtime::RocketMQRuntime;

let runtime = RocketMQRuntime::new_multi(2, "rocketmq-scheduler");
let mut runs = 0_u64;

runtime.schedule_at_fixed_rate_mut(
    move || {
        runs += 1;
    },
    None,
    Duration::from_secs(10),
);
```

## Runtime Model

| API | Purpose |
| --- | --- |
| `RocketMQRuntime::new_multi(threads, name)` | Creates a Tokio multi-thread runtime with the given worker count and thread name. |
| `get_handle()` | Returns a borrowed Tokio `Handle` for spawning async tasks. |
| `get_runtime()` | Returns a borrowed Tokio `Runtime` for lower-level integrations. |
| `schedule_at_fixed_rate(task, initial_delay, period)` | Runs an immutable synchronous closure repeatedly on the runtime. |
| `schedule_at_fixed_rate_mut(task, initial_delay, period)` | Runs a mutable synchronous closure repeatedly on the runtime. |
| `shutdown()` | Consumes the wrapper and performs Tokio background shutdown. |
| `shutdown_timeout(timeout)` | Consumes the wrapper and waits up to the supplied timeout for shutdown. |

The scheduler calculates each next run from the previous start time. If a task
takes longer than its configured period, the next delay saturates to zero and
the next run starts immediately after the current one finishes.

## Workspace Usage

Current workspace consumers use this crate for isolated background work:

- `rocketmq-broker` owns a broker runtime for broker-level background tasks and
  shuts it down from `BrokerRuntime::drop`.
- `rocketmq-broker` also creates a dedicated write-message runtime inside the
  pull-message processor.
- `rocketmq-client` exposes runtime injection for transaction producer check
  work.
- `rocketmq-controller`, `rocketmq-observability`, and `rocketmq-tieredstore`
  depend on the crate as part of the shared workspace runtime surface.

## Design Boundaries

- Only the multi-thread Tokio runtime variant is currently implemented.
- `new_multi` unwraps Tokio runtime construction. Callers should pass valid
  thread counts and use it where failing to create the runtime is fatal.
- Scheduled closures are synchronous. Keep them lightweight; when async work is
  required, capture a cloned Tokio `Handle` and call `spawn` from the closure.
- Scheduled loops run until the runtime is shut down. Components that need
  explicit cancellation should own task handles or use a higher-level task
  manager around this crate.
- The crate does not configure component-level metrics, tracing, or error
  handling policy.

## Crate Layout

```text
rocketmq-runtime/
  Cargo.toml        crate metadata and Tokio dependency
  src/lib.rs        RocketMQRuntime wrapper, handle access, shutdown, scheduling
```

## Validation

Useful checks for changes to this crate:

```bash
cargo test -p rocketmq-runtime --lib
cargo clippy -p rocketmq-runtime --all-targets --all-features -- -D warnings
```

Run the full workspace checks when runtime behavior changes can affect other
RocketMQ components.

## License

Licensed under the Apache License, Version 2.0. See
[`LICENSE-APACHE`](../LICENSE-APACHE) for details.
